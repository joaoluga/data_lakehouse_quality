import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType


@udf(StringType())
def denormalization(col, col_name):
    rules = {
        "TP_FAIXA_ETARIA": {
            None: None,
            1: "Menor de 17 anos",
            2: "17 anos",
            3: "18 anos",
            4: "19 anos",
            5: "20 anos",
            6: "21 anos",
            7: "22 anos",
            8: "23 anos",
            9: "24 anos",
            10: "25 anos",
            11: "Entre 26 e 30 anos",
            12: "Entre 31 e 35 anos",
            13: "Entre 36 e 40 anos",
            14: "Entre 41 e 45 anos",
            15: "Entre 46 e 50 anos",
            16: "Entre 51 e 55 anos",
            17: "Entre 56 e 60 anos",
            18: "Entre 61 e 65 anos",
            19: "Entre 66 e 70 anos",
            20: "Maior de 70 anos",
        },
        "TP_ESTADO_CIVIL": {
            None: None,
            0: "Não informado",
            1: "Solteiro(a)",
            2: "Casado(a)/Mora com companheiro(a)",
            3: "Divorciado(a)/Desquitado(a)/Separado(a)",
            4: "Viúvo(a)",
        },
        "TP_COR_RACA": {
            None: None,
            0: "Não declarado",
            1: "Branca",
            2: "Preta",
            3: "Parda",
            4: "Amarela",
            5: "Indígena",
            6: "Não dispõe da informação",
        },
        "TP_NACIONALIDADE": {
            None: None,
            0: "Não informado",
            1: "Brasileiro(a)",
            2: "Brasileiro(a) Naturalizado(a)",
            3: "Estrangeiro(a)",
            4: "Brasileiro(a) Nato(a), nascido(a) no exterior",
        },
        "TP_ST_CONCLUSAO": {
            None: None,
            1: "Já concluí o Ensino Médio",
            2: "Estou cursando e concluirei o Ensino Médio em 2022",
            3: "Estou cursando e concluirei o Ensino Médio após 2022",
            4: "Não concluí e não estou cursando o Ensino Médio",
        },
        "TP_ANO_CONCLUIU": {
            None: None,
            0: "Não informado",
            1: "2021",
            2: "2020",
            3: "2019",
            4: "2018",
            5: "2017",
            6: "2016",
            7: "2015",
            8: "2014",
            9: "2013",
            10: "2012",
            11: "2011",
            12: "2010",
            13: "2009",
            14: "2008",
            15: "2007",
            16: "Antes de 2007",
        },
        "TP_ESCOLA": {None: None, 1: "Não Respondeu", 2: "Pública", 3: "Privada"},
        "TP_ENSINO": {
            None: None,
            1: "Ensino Regular",
            2: "Educação Especial - Modalidade Substitutiva",
        },
        "IN_TREINEIRO": {None: None, 1: "Sim", 0: "Não"},
    }
    return rules[col_name][col]


class DimCandidato(ETLWithDataQuality):
    partitions = []
    mode = "upsert"
    merge_logic = "t.NU_INSCRICAO = s.NU_INSCRICAO"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "trusted"

    table_name = "dim_candidato"

    dataframe_view_name = "dim_candidato_df"

    primary_key = "NU_INSCRICAO"

    checks = f"""
checks for {dataframe_view_name}:
- row_count > 0
- missing_count(NU_INSCRICAO) = 0
- duplicate_count(NU_INSCRICAO) = 0
- missing_count(NU_ANO) = 0
- missing_count(TP_SEXO) = 0
- invalid_count(TP_SEXO) = 0:
    valid values: ['M', 'F']
- missing_count(TP_FAIXA_ETARIA) = 0
- invalid_count(TP_FAIXA_ETARIA) = 0:
    valid values: ['23 anos', 'Entre 26 e 30 anos', '24 anos', 'Entre 61 e
      65 anos', '21 anos', 'Entre 56 e 60 anos', '19 anos', '17 anos', '18
      anos', 'Menor de 17 anos', '22 anos', 'Entre 51 e 55 anos', 'Entre 46
      e 50 anos', 'Entre 31 e 35 anos', 'Entre 36 e 40 anos', '20 anos', 'Entre
      41 e 45 anos', '25 anos', 'Entre 66 e 70 anos', 'Maior de 70 anos']
- missing_count(TP_ESTADO_CIVIL) = 0
- invalid_count(TP_ESTADO_CIVIL) = 0:
    valid values: ['Não informado', 'Divorciado(a)/Desquitado(a)/Separado(a)',
      'Casado(a)/Mora com companheiro(a)', 'Solteiro(a)', 'Viúvo(a)']
- missing_count(TP_COR_RACA) = 0
- invalid_count(TP_COR_RACA) = 0:
    valid values: ['Branca', 'Amarela', 'Não declarado', 'Parda', 'Preta',
      'Indígena']
- missing_count(TP_NACIONALIDADE) = 0
- invalid_count(TP_NACIONALIDADE) = 0:
    valid values: ['Não informado', 'Brasileiro(a)', 'Brasileiro(a) Naturalizado(a)',
      'Brasileiro(a) Nato(a), nascido(a) no exterior', 'Estrangeiro(a)']
- missing_count(TP_ST_CONCLUSAO) = 0
- invalid_count(TP_ST_CONCLUSAO) = 0:
    valid values: ['Estou cursando e concluirei o Ensino Médio em 2022']
- missing_count(TP_ANO_CONCLUIU) = 0
- invalid_count(TP_ANO_CONCLUIU) = 0:
    valid values: ['Não informado']
- missing_count(TP_ESCOLA) = 0
- invalid_count(TP_ESCOLA) = 0:
    valid values: ['Privada', 'Pública']
- missing_count(TP_ENSINO) = 0
- invalid_count(TP_ENSINO) = 0:
    valid values: ['Educação Especial - Modalidade Substitutiva', 'Ensino Regular']
- missing_count(IN_TREINEIRO) = 0
- invalid_count(IN_TREINEIRO) = 0:
    valid values: ['Não']
"""
    post_check = None

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        df = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "raw", "tables", "enem_microdados"))
        )
        df = df.selectExpr(
            "NU_INSCRICAO",
            "NU_ANO",
            "TP_FAIXA_ETARIA",
            "TP_SEXO",
            "TP_ESTADO_CIVIL",
            "TP_COR_RACA",
            "TP_NACIONALIDADE",
            "TP_ST_CONCLUSAO",
            "TP_ANO_CONCLUIU",
            "TP_ESCOLA",
            "TP_ENSINO",
            "IN_TREINEIRO",
        )

        for norm_col in [
            "TP_FAIXA_ETARIA",
            "TP_ESTADO_CIVIL",
            "TP_COR_RACA",
            "TP_NACIONALIDADE",
            "TP_ST_CONCLUSAO",
            "TP_ANO_CONCLUIU",
            "TP_ESCOLA",
            "TP_ENSINO",
            "IN_TREINEIRO",
        ]:
            df = df.withColumn(
                f"{norm_col}_1", denormalization(df[norm_col], lit(norm_col))
            )
            df = df.drop(norm_col)
            df = df.withColumnRenamed(f"{norm_col}_1", norm_col)
        return df
