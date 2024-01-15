import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality
from pyspark.sql.functions import udf, lit, concat, md5, col
from pyspark.sql.types import StringType


@udf(StringType())
def denormalization_escolas(col, col_name):
    rules = {
        "TP_DEPENDENCIA_ADM_ESC": {
            None: None,
            1: "Federal",
            2: "Estadual",
            3: "Municipal",
            4: "Privada",
        },
        "TP_LOCALIZACAO_ESC": {None: None, 1: "Urbana", 2: "Rural"},
        "TP_SIT_FUNC_ESC": {
            None: None,
            1: "Em atividade",
            2: "Paralisada",
            3: "Extinta",
            4: "Escola extinta em anos anteriores",
        },
    }
    return rules[col_name][col]


class DimEscola(ETLWithDataQuality):
    partitions = []
    mode = "upsert"
    merge_logic = "t.id_escola = s.id_escola"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "trusted"

    table_name = "dim_escola"

    dataframe_view_name = "dim_escola_df"

    primary_key = "id_escola"

    checks = f"""
checks for {dataframe_view_name}:
- row_count > 0
- missing_count(CO_MUNICIPIO_ESC) = 0
- missing_count(NO_MUNICIPIO_ESC) = 0
- missing_count(SG_UF_ESC) = 0
- invalid_count(SG_UF_ESC) = 0:
    valid values: ['SC', 'BA', 'MS', 'PA', 'AP', 'PE', 'AL', 'MT',
      'AC', 'SP', 'PI', 'RS', 'RR', 'MG', 'GO', 'CE', 'RN', 'MA',
      'SE', 'PR', 'AM', 'ES', 'PB', 'RO', 'DF', 'RJ', 'TO']
- missing_count(TP_DEPENDENCIA_ADM_ESC) = 0
- invalid_count(TP_DEPENDENCIA_ADM_ESC) = 0:
    valid values: ['Privada', 'Estadual', 'Municipal', 'Federal']
- missing_count(TP_LOCALIZACAO_ESC) = 0
- invalid_count(TP_LOCALIZACAO_ESC) = 0:
    valid values: ['Urbana', 'Rural']
- missing_count(TP_SIT_FUNC_ESC) = 0
- invalid_count(TP_SIT_FUNC_ESC) = 0:
    valid values: ['Em atividade', 'Extinta', 'Escola extinta em anos anteriores',
      'Paralisada']
    """

    post_check = None

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        df = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "raw", "tables", "enem_microdados"))
        )
        df = df.selectExpr(
            "CO_MUNICIPIO_ESC",
            "NO_MUNICIPIO_ESC",
            "SG_UF_ESC",
            "TP_DEPENDENCIA_ADM_ESC",
            "TP_LOCALIZACAO_ESC",
            "TP_SIT_FUNC_ESC",
        )

        columns_to_md5 = df.columns
        df = df.withColumn(
            "escola_id", md5(concat(*[col(coluna) for coluna in columns_to_md5]))
        )

        for norm_col in [
            "TP_DEPENDENCIA_ADM_ESC",
            "TP_LOCALIZACAO_ESC",
            "TP_SIT_FUNC_ESC",
        ]:
            df = df.withColumn(
                f"{norm_col}_1", denormalization_escolas(df[norm_col], lit(norm_col))
            )
            df = df.drop(norm_col)
            df = df.withColumnRenamed(f"{norm_col}_1", norm_col)

        df = df.drop_duplicates()
        return df
