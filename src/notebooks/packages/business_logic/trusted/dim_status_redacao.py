import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def denormalization_redacao(col):
    rules = {
        None: None,
        1: "Sem problemas",
        2: "Anulada",
        3: "Cópia Texto Motivador",
        4: "Em Branco",
        6: "Fuga ao tema",
        7: "Não atendimento ao tipo textual",
        8: "Texto insuficiente",
        9: "Parte desconectada",
    }
    return rules[col]


class DimStatusRedacao(ETLWithDataQuality):
    partitions = []
    mode = "upsert"
    merge_logic = "t.TP_STATUS_REDACAO = s.TP_STATUS_REDACAO"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "trusted"

    table_name = "dim_status_redacao"

    dataframe_view_name = "dim_status_redacao_df"

    checks = f"""checks for {dataframe_view_name}:
- missing_count(TP_STATUS_REDACAO) = 0
- duplicate_count(TP_STATUS_REDACAO) = 0
- duplicate_count(TP_STATUS_REDACAO) = 0
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid min: '1'
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid max: '9'
- missing_count(DESCRICAO) = 0
- duplicate_count(DESCRICAO) = 0
    """

    primary_key = "TP_STATUS_REDACAO"

    post_check = None

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        df = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "raw", "tables", "enem_microdados"))
        )
        df = df.selectExpr("TP_STATUS_REDACAO")
        df = df.withColumn(
            f"DESCRICAO", denormalization_redacao(df["TP_STATUS_REDACAO"])
        )
        df = df.drop_duplicates()
        return df
