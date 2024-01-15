import os
from abc import abstractmethod, ABC
from functools import reduce

import pyspark
from packages.etl.loader import Loader
from packages.utils.logger import Logger
from pyspark.sql.functions import lit, collect_list, sum, array, create_map
from soda.scan import Scan


class ETLWithDataQuality(ABC):
    @property
    @abstractmethod
    def partitions(self):
        pass

    @property
    @abstractmethod
    def update_logic(self):
        pass

    @property
    @abstractmethod
    def merge_logic(self):
        pass

    @property
    @abstractmethod
    def mode(self):
        pass

    @property
    @abstractmethod
    def target_layer(self):
        pass

    @property
    @abstractmethod
    def table_name(self):
        pass

    @property
    @abstractmethod
    def dataframe_view_name(self):
        pass

    @property
    @abstractmethod
    def checks(self):
        pass

    @property
    @abstractmethod
    def post_check(self):
        pass

    @property
    @abstractmethod
    def primary_key(self):
        pass

    def __init__(self, spark_session, base_path):
        self._logger = Logger()
        self._loader = Loader(spark=spark_session)
        self.spark_session = spark_session
        self.base_path = base_path

    @abstractmethod
    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        pass

    def __set_soda_scan(self, target_checks):
        scan = Scan()
        scan.set_scan_definition_name("Datasets validation")
        scan.set_data_source_name("spark_df")
        scan.add_spark_session(self.spark_session)
        scan.add_sodacl_yaml_str(target_checks)
        scan.execute()
        return scan

    def __get_failed_quality_records(self):
        scan = self.__set_soda_scan(self.checks)
        df_final = None  # Inicializa o dataframe final como nulo
        df_list = []
        if scan.has_check_fails():
            for check_failed in scan.get_checks_fail():
                check_failed = check_failed.dict
                failed_check_name = check_failed["metrics"][0].split("-")[-1]
                query_name = f"{check_failed['dataSource']}.{check_failed['column']}.failed_rows[{failed_check_name}].failing_sql"
                query = next(
                    (
                        query_info
                        for query_info in scan.get_scan_results()["queries"]
                        if query_info.get("name") == query_name
                    ),
                    None,
                )
                if not query:
                    failed_check_name = check_failed["metrics"][0].split("-")[-2]
                    query_name = f"{check_failed['dataSource']}.{check_failed['column']}.failed_rows[{failed_check_name}].failing_sql"
                    query = next(
                        (
                            query_info
                            for query_info in scan.get_scan_results()["queries"]
                            if query_info.get("name") == query_name
                        ),
                        None,
                    )
                # print(f"\nfailed_check_name: {failed_check_name}")
                # print(f"query_name: {query_name}")
                # print(f"query: {query}")
                df = (
                    self.spark_session.sql(query["sql"])
                    .withColumn(
                        "quality_errors",
                        array(
                            create_map(
                                lit(check_failed["column"]), lit(failed_check_name)
                            )
                        ),
                    )
                    .withColumn("total_quality_errors", lit(1))
                )

                df_list.append(df)

            df_final = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
            df_final = df_final.groupBy(
                *[
                    col
                    for col in df_final.columns
                    if col not in ["quality_errors", "total_quality_errors"]
                ]
            ).agg(
                collect_list("quality_errors").alias("error_list"),
                sum("total_quality_errors").alias("total_quality_errors"),
            )
            return df_final
        else:
            return None

    def __quality_workflow(self, df, include_dq_workflow=True):
        print("Starting process")
        df.createOrReplaceTempView(self.dataframe_view_name)
        if include_dq_workflow:
            quality_check_df = self.__get_failed_quality_records()
        else:
            quality_check_df = False

        delta_path = os.path.join(
            self.base_path, "layers", self.target_layer, "tables", self.table_name
        )
        if quality_check_df:
            self._logger.info("Found errors...")
            self._logger.info("Writting incorrect data to error table")
            delta_path_error = os.path.join(
                self.base_path,
                "layers",
                self.target_layer,
                "tables",
                f"{self.table_name}_errors",
            )
            self._logger.info(f"Delta Path Error: {delta_path_error}")
            self._loader.write(
                df=quality_check_df,
                path=delta_path_error,
                mode=self.mode,
                merge_logic=self.merge_logic,
                update_logic=self.update_logic,
                partitions=self.partitions,
            )
            self._logger.info("Cleaning wrong data")
            quality_check_df = quality_check_df.drop(
                *["error_list", "total_quality_errors"]
            )
            quality_check_df = quality_check_df.drop_duplicates()
            self._logger.info("Subtracting data from original df")
            df = df.repartition(self.primary_key)
            quality_check_df = quality_check_df.repartition(self.primary_key)
            # self._logger.info(f"Total original_df: {df.count()}")
            # self._logger.info(
            #     f"Total incorrect errors found: {quality_check_df.count()}"
            # )
            final_df = df.subtract(quality_check_df)
            final_df_count = final_df.count()
            self._logger.info(f"Total correct data found: {final_df_count}")
            self._logger.info("Writing original df")
            self._logger.info(f"Delta Path: {delta_path}")
            if final_df_count > 0:
                self._loader.write(
                    df=final_df,
                    path=delta_path,
                    mode=self.mode,
                    merge_logic=self.merge_logic,
                    update_logic=self.update_logic,
                    partitions=self.partitions,
                )
        else:
            self._logger.info("No errors found")
            self._logger.info("Writing original df")
            self._loader.write(
                df=df,
                path=delta_path,
                mode=self.mode,
                merge_logic=self.merge_logic,
                update_logic=self.update_logic,
                partitions=self.partitions,
            )

    def execute(self, include_dq_workflow=True):
        df = self.data_transform_workflow()
        self.__quality_workflow(df, include_dq_workflow=include_dq_workflow)
