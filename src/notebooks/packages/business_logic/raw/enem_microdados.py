import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality


class EnemMicrodadosWorkflow(ETLWithDataQuality):
    partitions = ["NU_ANO"]
    mode = "upsert"
    merge_logic = "t.NU_INSCRICAO = s.NU_INSCRICAO AND t.NU_ANO = s.NU_ANO"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "raw"

    table_name = "enem_microdados"

    dataframe_view_name = "enem_df"

    primary_key = "NU_INSCRICAO"

    checks = f"""
checks for {dataframe_view_name}:
- row_count > 0
- missing_count(NU_INSCRICAO) = 0
- duplicate_count(NU_INSCRICAO) = 0
- missing_count(NU_ANO) = 0
- missing_count(TP_FAIXA_ETARIA) = 0
- invalid_count(TP_FAIXA_ETARIA) = 0:
    valid min: '1'
- invalid_count(TP_FAIXA_ETARIA) = 0:
    valid max: '20'
- missing_count(TP_SEXO) = 0
- invalid_count(TP_SEXO) = 0:
    valid values: ['M', 'F']
- missing_count(TP_ESTADO_CIVIL) = 0
- invalid_count(TP_ESTADO_CIVIL) = 0:
    valid min: '0'
- invalid_count(TP_ESTADO_CIVIL) = 0:
    valid max: '4'
- invalid_count(TP_ESTADO_CIVIL) = 0:
    valid values: [2, 4, 1, 0, 3]
- missing_count(TP_COR_RACA) = 0
- invalid_count(TP_COR_RACA) = 0:
    valid min: '0'
- invalid_count(TP_COR_RACA) = 0:
    valid max: '5'
- invalid_count(TP_COR_RACA) = 0:
    valid values: [2, 5, 4, 1, 3, 0, 6]
- missing_count(TP_NACIONALIDADE) = 0
- invalid_count(TP_NACIONALIDADE) = 0:
    valid min: '0'
- invalid_count(TP_NACIONALIDADE) = 0:
    valid max: '4'
- invalid_count(TP_NACIONALIDADE) = 0:
    valid values: [2, 4, 1, 3, 0]
- missing_count(TP_ST_CONCLUSAO) = 0
- invalid_count(TP_ST_CONCLUSAO) = 0:
    valid min: '1'
- invalid_count(TP_ST_CONCLUSAO) = 0:
    valid max: '4'
- invalid_count(TP_ST_CONCLUSAO) = 0:
    valid values: [2, 4, 1, 3]
- missing_count(TP_ANO_CONCLUIU) = 0
- invalid_count(TP_ANO_CONCLUIU) = 0:
    valid min: '0'
- invalid_count(TP_ANO_CONCLUIU) = 0:
    valid max: '16'
- missing_count(TP_ESCOLA) = 0
- invalid_count(TP_ESCOLA) = 0:
    valid min: '1'
- invalid_count(TP_ESCOLA) = 0:
    valid max: '3'
- invalid_count(TP_ESCOLA) = 0:
    valid values: [2, 1, 3]
- missing_count(TP_ENSINO) = 0
- invalid_count(TP_ENSINO) = 0:
    valid min: '1'
- invalid_count(TP_ENSINO) = 0:
    valid max: '2'
- invalid_count(TP_ENSINO) = 0:
    valid values: [ 2, 1]
- missing_count(IN_TREINEIRO) = 0
- invalid_count(IN_TREINEIRO) = 0:
    valid min: '0'
- invalid_count(IN_TREINEIRO) = 0:
    valid max: '1'
- invalid_count(IN_TREINEIRO) = 0:
    valid values: [1, 0]
- missing_count(CO_MUNICIPIO_ESC) = 0
- missing_count(NO_MUNICIPIO_ESC) = 0
- missing_count(CO_UF_ESC) = 0
- missing_count(SG_UF_ESC) = 0
- invalid_count(SG_UF_ESC) = 0:
    valid values: [ 'BA', 'SC', 'MS', 'PA', 'MT', 'AL', 'PE',
      'AP', 'SP', 'RS', 'AC', 'PI', 'SE', 'CE', 'GO', 'RN', 'MA',
      'MG', 'RR', 'AM', 'ES', 'PB', 'PR', 'RJ', 'DF', 'RO', 'TO']
- missing_count(TP_DEPENDENCIA_ADM_ESC) = 0
- invalid_count(TP_DEPENDENCIA_ADM_ESC) = 0:
    valid min: '1'
- invalid_count(TP_DEPENDENCIA_ADM_ESC) = 0:
    valid max: '4'
- invalid_count(TP_DEPENDENCIA_ADM_ESC) = 0:
    valid values: [ 2, 4, 1, 3]
- missing_count(TP_LOCALIZACAO_ESC) = 0
- invalid_count(TP_LOCALIZACAO_ESC) = 0:
    valid min: '1'
- invalid_count(TP_LOCALIZACAO_ESC) = 0:
    valid max: '2'
- invalid_count(TP_LOCALIZACAO_ESC) = 0:
    valid values: [ 2, 1]
- missing_count(TP_SIT_FUNC_ESC) = 0
- invalid_count(TP_SIT_FUNC_ESC) = 0:
    valid min: '1'
- invalid_count(TP_SIT_FUNC_ESC) = 0:
    valid max: '4'
- invalid_count(TP_SIT_FUNC_ESC) = 0:
    valid values: [ 2, 4, 1, 3]
- missing_count(CO_MUNICIPIO_PROVA) = 0
- missing_count(NO_MUNICIPIO_PROVA) = 0
- missing_count(CO_UF_PROVA) = 0
- invalid_count(CO_UF_PROVA) = 0:
    valid min: '11'
- invalid_count(CO_UF_PROVA) = 0:
    valid max: '53'
- missing_count(SG_UF_PROVA) = 0
- invalid_count(SG_UF_PROVA) = 0:
    valid values: ['BA', 'MS', 'SC', 'PA', 'PE', 'MT', 'AL', 'AP',
      'SP', 'RS', 'AC', 'PI', 'SE', 'MG', 'CE', 'RN', 'MA', 'GO',
      'RR', 'ES', 'PB', 'AM', 'PR', 'DF', 'RJ', 'TO', 'RO']
- missing_count(TP_PRESENCA_CN) = 0
- invalid_count(TP_PRESENCA_CN) = 0:
    valid min: '0'
- invalid_count(TP_PRESENCA_CN) = 0:
    valid max: '2'
- invalid_count(TP_PRESENCA_CN) = 0:
    valid values: [2, 1, 0]
- missing_count(TP_PRESENCA_CH) = 0
- invalid_count(TP_PRESENCA_CH) = 0:
    valid min: '0'
- invalid_count(TP_PRESENCA_CH) = 0:
    valid max: '2'
- invalid_count(TP_PRESENCA_CH) = 0:
    valid values: [2, 1, 0]
- missing_count(TP_PRESENCA_LC) = 0
- invalid_count(TP_PRESENCA_LC) = 0:
    valid min: '0'
- invalid_count(TP_PRESENCA_LC) = 0:
    valid max: '2'
- invalid_count(TP_PRESENCA_LC) = 0:
    valid values: [2, 1, 0]
- missing_count(TP_PRESENCA_MT) = 0
- invalid_count(TP_PRESENCA_MT) = 0:
    valid min: '0'
- invalid_count(TP_PRESENCA_MT) = 0:
    valid max: '2'
- invalid_count(TP_PRESENCA_MT) = 0:
    valid values: [2, 1, 0]
- missing_count(CO_PROVA_CN) = 0
- missing_count(CO_PROVA_CH) = 0
- missing_count(CO_PROVA_LC) = 0
- missing_count(CO_PROVA_MT) = 0
- missing_count(NU_NOTA_CN) = 0
- invalid_count(NU_NOTA_CN) = 0:
    valid min: '0.0'
- invalid_count(NU_NOTA_CN) = 0:
    valid max: '1000'
- missing_count(NU_NOTA_CH) = 0
- invalid_count(NU_NOTA_CH) = 0:
    valid min: '0.0'
- invalid_count(NU_NOTA_CH) = 0:
    valid max: '1000'
- missing_count(NU_NOTA_LC) = 0
- invalid_count(NU_NOTA_LC) = 0:
    valid min: '0.0'
- invalid_count(NU_NOTA_LC) = 0:
    valid max: '1000'
- missing_count(NU_NOTA_MT) = 0
- invalid_count(NU_NOTA_MT) = 0:
    valid min: '0.0'
- invalid_count(NU_NOTA_MT) = 0:
    valid max: '1000'
- missing_count(TX_RESPOSTAS_CN) = 0
- missing_count(TX_RESPOSTAS_CH) = 0
- missing_count(TX_RESPOSTAS_LC) = 0
- missing_count(TX_RESPOSTAS_MT) = 0
- missing_count(TP_LINGUA) = 0
- invalid_count(TP_LINGUA) = 0:
    valid min: '0'
- invalid_count(TP_LINGUA) = 0:
    valid max: '1'
- invalid_count(TP_LINGUA) = 0:
    valid values: [1, 0]
- missing_count(TX_GABARITO_CN) = 0
- missing_count(TX_GABARITO_CH) = 0
- missing_count(TX_GABARITO_LC) = 0
- missing_count(TX_GABARITO_MT) = 0
- missing_count(TP_STATUS_REDACAO) = 0
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid min: '1'
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid max: '9'
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid values: [ 4, 2, 1, 6, 3, 9, 8, 7]
- missing_count(NU_NOTA_COMP1) = 0
- invalid_count(NU_NOTA_COMP1) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_COMP1) = 0:
    valid max: '200'
- missing_count(NU_NOTA_COMP2) = 0
- invalid_count(NU_NOTA_COMP2) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_COMP2) = 0:
    valid max: '200'
- missing_count(NU_NOTA_COMP3) = 0
- invalid_count(NU_NOTA_COMP3) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_COMP3) = 0:
    valid max: '200'
- missing_count(NU_NOTA_COMP4) = 0
- invalid_count(NU_NOTA_COMP4) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_COMP4) = 0:
    valid max: '200'
- missing_count(NU_NOTA_COMP5) = 0
- invalid_count(NU_NOTA_COMP5) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_COMP5) = 0:
    valid max: '200'
- missing_count(NU_NOTA_REDACAO) = 0
- invalid_count(NU_NOTA_REDACAO) = 0:
    valid min: '0'
- invalid_count(NU_NOTA_REDACAO) = 0:
    valid max: '1000'
- missing_count(Q001) = 0
- invalid_count(Q001) = 0:
    valid values: ['G', 'H', 'E', 'C', 'A', 'D', 'B', 'F']
- missing_count(Q002) = 0
- invalid_count(Q002) = 0:
    valid values: ['H', 'G', 'C', 'E', 'A', 'D', 'B', 'F']
- missing_count(Q003) = 0
- invalid_count(Q003) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B', 'F']
- missing_count(Q004) = 0
- invalid_count(Q004) = 0:
    valid values: ['E', 'C', 'D', 'A', 'B', 'F']
- missing_count(Q005) = 0
- invalid_count(Q005) = 0:
    valid min: '1'
- invalid_count(Q005) = 0:
    valid max: '20'
- missing_count(Q006) = 0
- invalid_count(Q006) = 0:
    valid values: ['G', 'H', 'I', 'K', 'O', 'M', 'C', 'E', 'A',
      'D', 'N', 'B', 'F', 'L', 'J', 'Q', 'P']
- missing_count(Q007) = 0
- invalid_count(Q007) = 0:
    valid values: ['C', 'D', 'A', 'B']
- missing_count(Q008) = 0
- invalid_count(Q008) = 0:
    valid values: ['E', 'C', 'D', 'A', 'B']
- missing_count(Q009) = 0
- invalid_count(Q009) = 0:
    valid values: ['E', 'C', 'D', 'A', 'B']
- missing_count(Q010) = 0
- invalid_count(Q010) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q011) = 0
- invalid_count(Q011) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q012) = 0
- invalid_count(Q012) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q013) = 0
- invalid_count(Q013) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q014) = 0
- invalid_count(Q014) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q015) = 0
- invalid_count(Q015) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q016) = 0
- invalid_count(Q016) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q017) = 0
- invalid_count(Q017) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q018) = 0
- invalid_count(Q018) = 0:
    valid values: ['A', 'B']
- missing_count(Q019) = 0
- invalid_count(Q019) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q020) = 0
- invalid_count(Q020) = 0:
    valid values: ['A', 'B']
- missing_count(Q021) = 0
- invalid_count(Q021) = 0:
    valid values: ['A', 'B']
- missing_count(Q022) = 0
- invalid_count(Q022) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q023) = 0
- invalid_count(Q023) = 0:
    valid values: ['A', 'B']
- missing_count(Q024) = 0
- invalid_count(Q024) = 0:
    valid values: ['E', 'C', 'A', 'D', 'B']
- missing_count(Q025) = 0
- invalid_count(Q025) = 0:
    valid values: ['A', 'B']
"""
    post_check = None

    def __init__(self, year, spark_session, base_path):
        self.year = year
        super().__init__(spark_session, base_path)

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        print(self.year)
        enem_df = self.spark_session.read.csv(
            os.path.join(
                os.getcwd(),
                "layers",
                "landing",
                "enem",
                str(self.year),
                f"MICRODADOS_ENEM_{self.year}.csv",
            ),
            header=True,
            inferSchema=True,
            sep=";",
            encoding="latin1",
        )

        return enem_df
