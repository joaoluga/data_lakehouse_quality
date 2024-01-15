from __future__ import annotations

from pyspark.sql.functions import lit, year, month, dayofmonth
from delta import DeltaTable
from packages.utils.logger import Logger
from datetime import datetime


class Loader:
    def __init__(self, spark) -> None:
        self._logger = Logger()
        self.spark = spark
        super().__init__()

    def check_if_table_exists(self, spark, path):
        try:
            delta_table = DeltaTable.forPath(spark, path)
            return True
        except:
            return False

    def __apply_system_columns(self, df):
        self._logger.info("Applying system columns")
        df = df.withColumn("ingestion_at", lit(datetime.now()))
        df = df.withColumn("YEAR", year("ingestion_at"))
        df = df.withColumn("MONTH", month("ingestion_at"))
        df = df.withColumn("DAY", dayofmonth("ingestion_at"))
        return df

    def write(self, df, path, mode, merge_logic, update_logic, partitions):
        df = self.__apply_system_columns(df=df)
        if not self.check_if_table_exists(self.spark, path=path):
            df.write.partitionBy(*partitions).format("delta").mode("overwrite").save(
                path
            )
            return

        if mode == "overwrite":
            df.write.format("delta").mode("overwrite").save(path)
        elif mode == "upsert":
            delta_table = DeltaTable.forPath(self.spark, path)
            delta_table.alias("t").merge(
                df.alias("s"), merge_logic
            ).whenMatchedUpdateAll(update_logic).whenNotMatchedInsertAll().execute()
        elif mode == "append":
            df.write.format("delta").mode("append").save(path)
        else:
            raise Exception("Set valid mode type: overwrite, append, upsert")
