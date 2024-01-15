import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality


class FactCandidatoDenormalized(ETLWithDataQuality):
    partitions = []
    mode = "upsert"
    merge_logic = "t.NU_INSCRICAO = s.NU_INSCRICAO"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "analytics"

    table_name = "fact_candidato_denormalized"

    dataframe_view_name = "fact_candidato_denormalized_df"

    primary_key = "NU_INSCRICAO"

    checks = f"""checks for {dataframe_view_name}:
- missing_count(NU_INSCRICAO) = 0
- duplicate_count(NU_INSCRICAO) = 0
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
- missing_count(escola_id) = 0
- missing_count(NU_ANO) = 0
- missing_count(TP_SEXO) = 0
- invalid_count(TP_SEXO) = 0:
    valid values: ['M', 'F']
- missing_count(TP_FAIXA_ETARIA) = 0
- invalid_count(TP_FAIXA_ETARIA) = 0:
    valid values: ['23 anos', 'Entre 26 e 30 anos', '24 anos', 'Entre 61 e
      65 anos', 'Maior de 70 anos', 'Entre 56 e 60 anos', '21 anos', 'Entre
      51 e 55 anos', 'Menor de 17 anos', '17 anos', 'Entre 46 e 50 anos', '18
      anos', '19 anos', '22 anos', 'Entre 36 e 40 anos', 'Entre 31 e 35 anos',
      '20 anos', 'Entre 41 e 45 anos', '25 anos', 'Entre 66 e 70 anos']
- missing_count(TP_ESTADO_CIVIL) = 0
- invalid_count(TP_ESTADO_CIVIL) = 0:
    valid values: ['Não informado', 'Divorciado(a)/Desquitado(a)/Separado(a)',
      'Casado(a)/Mora com companheiro(a)', 'Solteiro(a)', 'Viúvo(a)']
- missing_count(TP_COR_RACA) = 0
- invalid_count(TP_COR_RACA) = 0:
    valid values: ['Branca', 'Não declarado', 'Amarela', 'Parda', 'Preta',
      'Indígena']
- missing_count(TP_NACIONALIDADE) = 0
- invalid_count(TP_NACIONALIDADE) = 0:
    valid values: ['Não informado', 'Brasileiro(a)', 'Brasileiro(a) Naturalizado(a)',
      'Brasileiro(a) Nato(a), nascido(a) no exterior', 'Estrangeiro(a)']
- missing_count(TP_ST_CONCLUSAO) = 0
- invalid_count(TP_ST_CONCLUSAO) = 0:
    valid values: ['Estou cursando e concluirei o Ensino Médio em 2022', 'Estou
      cursando e concluirei o Ensino Médio após 2022', 'Já concluí o Ensino Médio',
      'Não concluí e não estou cursando o Ensino Médio']
- missing_count(TP_ANO_CONCLUIU) = 0
- invalid_count(TP_ANO_CONCLUIU) = 0:
    valid values: ['Não informado', '2012', '2020', '2019', '2014', '2015',
      '2008', '2018', '2007', '2011', '2009', '2010', '2016', '2017',
      'Antes de 2007', '2013', '2021']
- missing_count(TP_ESCOLA) = 0
- invalid_count(TP_ESCOLA) = 0:
    valid values: ['Privada', 'Pública', 'Não Respondeu']
- missing_count(TP_ENSINO) = 0
- invalid_count(TP_ENSINO) = 0:
    valid values: [ 'Educação Especial - Modalidade Substitutiva', 'Ensino
      Regular']
- missing_count(IN_TREINEIRO) = 0
- invalid_count(IN_TREINEIRO) = 0:
    valid values: ['Não', 'Sim']
- missing_count(NO_MUNICIPIO_ESC) = 0
- missing_count(SG_UF_ESC) = 0
- invalid_count(SG_UF_ESC) = 0:
    valid values: [ 'MS', 'BA', 'SC', 'PA', 'AP', 'MT', 'PE',
      'AL', 'AC', 'RS', 'SP', 'PI', 'MG', 'MA', 'GO', 'CE', 'RR',
      'RN', 'SE', 'AM', 'ES', 'PR', 'PB', 'TO', 'RO', 'RJ', 'DF']
- missing_count(TP_DEPENDENCIA_ADM_ESC) = 0
- invalid_count(TP_DEPENDENCIA_ADM_ESC) = 0:
    valid values: [ 'Privada', 'Estadual', 'Municipal', 'Federal']
- missing_count(TP_LOCALIZACAO_ESC) = 0
- invalid_count(TP_LOCALIZACAO_ESC) = 0:
    valid values: [ 'Urbana', 'Rural']
- missing_count(TP_SIT_FUNC_ESC) = 0
- invalid_count(TP_SIT_FUNC_ESC) = 0:
    valid values: [ 'Em atividade', 'Extinta', 'Escola extinta em anos
      anteriores', 'Paralisada']
- missing_count(TP_STATUS_REDACAO) = 0
- invalid_count(TP_STATUS_REDACAO) = 0:
    valid values: ['Sem problemas', 'Parte desconectada', 'Não atendimento ao
      tipo textual', 'Anulada', 'Cópia Texto Motivador', 'Texto insuficiente',
      'Em Branco', 'Fuga ao tema']
- missing_count(CO_PROVA_CH) = 0
- invalid_count(CO_PROVA_CH) = 0:
    valid values: ['Amarela', 'Amarela (Digital)', 'Amarela (Reaplicação)', 'Amarela (Segunda oportunidade)',
 'Amarelo (Reaplicação)', 'Azul', 'Azul (Digital)', 'Azul (Reaplicação)', 'Azul (Segunda oportunidade)', 'Branca',
 'Branca (Digital)', 'Branca (Reaplicação)', 'Branca (Segunda oportunidade)', 'Branco (Reaplicação)',
 'Cinza', 'Cinza (Digital)', 'Cinza (Reaplicação)', 'Cinza (Segunda oportunidade)',
 'Laranja - Adaptada Ledor', 'Laranja - Adaptada Ledor (Reaplicação)', 'Laranja - Adaptada Ledor (Segunda oportunidade)',
 'Laranja - Braille', 'Rosa', 'Rosa (Digital)', 'Rosa (Reaplicação)',
 'Rosa (Segunda oportunidade)', 'Rosa - Ampliada', 'Rosa - Superampliada', 'Verde - Videoprova - Libras']
- missing_count(CO_PROVA_LC) = 0
- invalid_count(CO_PROVA_LC) = 0:
    valid values: ['Amarela', 'Amarela (Digital)', 'Amarela (Reaplicação)', 'Amarela (Segunda oportunidade)',
 'Amarelo (Reaplicação)', 'Azul', 'Azul (Digital)', 'Azul (Reaplicação)', 'Azul (Segunda oportunidade)', 'Branca',
 'Branca (Digital)', 'Branca (Reaplicação)', 'Branca (Segunda oportunidade)', 'Branco (Reaplicação)',
 'Cinza', 'Cinza (Digital)', 'Cinza (Reaplicação)', 'Cinza (Segunda oportunidade)',
 'Laranja - Adaptada Ledor', 'Laranja - Adaptada Ledor (Reaplicação)', 'Laranja - Adaptada Ledor (Segunda oportunidade)',
 'Laranja - Braille', 'Rosa', 'Rosa (Digital)', 'Rosa (Reaplicação)',
 'Rosa (Segunda oportunidade)', 'Rosa - Ampliada', 'Rosa - Superampliada', 'Verde - Videoprova - Libras']
- missing_count(CO_PROVA_MT) = 0
- invalid_count(CO_PROVA_MT) = 0:
    valid values: ['Amarela', 'Amarela (Digital)', 'Amarela (Reaplicação)', 'Amarela (Segunda oportunidade)',
 'Amarelo (Reaplicação)', 'Azul', 'Azul (Digital)', 'Azul (Reaplicação)', 'Azul (Segunda oportunidade)', 'Branca',
 'Branca (Digital)', 'Branca (Reaplicação)', 'Branca (Segunda oportunidade)', 'Branco (Reaplicação)',
 'Cinza', 'Cinza (Digital)', 'Cinza (Reaplicação)', 'Cinza (Segunda oportunidade)',
 'Laranja - Adaptada Ledor', 'Laranja - Adaptada Ledor (Reaplicação)', 'Laranja - Adaptada Ledor (Segunda oportunidade)',
 'Laranja - Braille', 'Rosa', 'Rosa (Digital)', 'Rosa (Reaplicação)',
 'Rosa (Segunda oportunidade)', 'Rosa - Ampliada', 'Rosa - Superampliada', 'Verde - Videoprova - Libras']
- missing_count(CO_PROVA_CN) = 0
- invalid_count(CO_PROVA_CN) = 0:
    valid values: ['Amarela', 'Amarela (Digital)', 'Amarela (Reaplicação)', 'Amarela (Segunda oportunidade)',
 'Amarelo (Reaplicação)', 'Azul', 'Azul (Digital)', 'Azul (Reaplicação)', 'Azul (Segunda oportunidade)', 'Branca',
 'Branca (Digital)', 'Branca (Reaplicação)', 'Branca (Segunda oportunidade)', 'Branco (Reaplicação)',
 'Cinza', 'Cinza (Digital)', 'Cinza (Reaplicação)', 'Cinza (Segunda oportunidade)',
 'Laranja - Adaptada Ledor', 'Laranja - Adaptada Ledor (Reaplicação)', 'Laranja - Adaptada Ledor (Segunda oportunidade)',
 'Laranja - Braille', 'Rosa', 'Rosa (Digital)', 'Rosa (Reaplicação)',
 'Rosa (Segunda oportunidade)', 'Rosa - Ampliada', 'Rosa - Superampliada', 'Verde - Videoprova - Libras']
    """

    post_check = None

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        # -- dim_candidato
        dim_candidato = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "trusted", "tables", "dim_candidato"))
        )
        dim_candidato = dim_candidato.withColumnRenamed(
            "NU_INSCRICAO", "NU_INSCRICAO_1"
        )
        dim_candidato = dim_candidato.drop("ingestion_at", "year", "month", "day")
        # -- dim_escola
        dim_escola = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "trusted", "tables", "dim_escola"))
        )
        dim_escola = dim_escola.withColumnRenamed("escola_id", "escola_id_1")
        dim_escola = dim_escola.withColumnRenamed(
            "CO_MUNICIPIO_ESC", "CO_MUNICIPIO_ESC_1"
        )
        dim_escola = dim_escola.drop("ingestion_at", "year", "month", "day")
        # -- dim_status_redacao
        dim_status_redacao = self.spark_session.read.format("delta").load(
            (
                os.path.join(
                    os.getcwd(), "layers", "trusted", "tables", "dim_status_redacao"
                )
            )
        )
        dim_status_redacao = dim_status_redacao.withColumnRenamed(
            "TP_STATUS_REDACAO", "TP_STATUS_REDACAO_1"
        )
        dim_status_redacao = dim_status_redacao.drop(
            "ingestion_at", "year", "month", "day"
        )
        # -- dim_tipo_prova
        dim_tipo_prova = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "trusted", "tables", "dim_tipo_prova"))
        )
        dim_tipo_prova = dim_tipo_prova.drop("ingestion_at", "year", "month", "day")
        # -- df_fact
        df_fact_denormalized = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "trusted", "tables", "fact_candidato"))
        )
        df_fact_denormalized = df_fact_denormalized.drop(
            "ingestion_at", "year", "month", "day"
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_candidato,
            [dim_candidato.NU_INSCRICAO_1 == df_fact_denormalized.NU_INSCRICAO],
            "LEFT",
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_escola,
            [dim_escola.escola_id_1 == df_fact_denormalized.escola_id],
            "LEFT",
        )
        df_fact_denormalized = (
            df_fact_denormalized.join(
                dim_status_redacao,
                [
                    dim_status_redacao.TP_STATUS_REDACAO_1
                    == df_fact_denormalized.TP_STATUS_REDACAO
                ],
                "LEFT",
            )
            .drop("TP_STATUS_REDACAO")
            .withColumnRenamed("DESCRICAO", "TP_STATUS_REDACAO")
        )
        df_fact_denormalized = df_fact_denormalized.drop(
            "NU_INSCRICAO_1", "CO_MUNICIPIO_ESC_1", "TP_STATUS_REDACAO_1", "escola_id_1"
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_tipo_prova,
            [dim_tipo_prova.CODIGO_PROVA == df_fact_denormalized.CO_PROVA_CH],
            "LEFT",
        ).drop(*["CO_PROVA_CH", "CO_PROVA", "CODIGO_PROVA"])
        df_fact_denormalized = df_fact_denormalized.withColumnRenamed(
            "DESCRICAO", "CO_PROVA_CH"
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_tipo_prova,
            [dim_tipo_prova.CODIGO_PROVA == df_fact_denormalized.CO_PROVA_LC],
            "LEFT",
        ).drop(*["CO_PROVA_LC", "CO_PROVA", "CODIGO_PROVA"])
        df_fact_denormalized = df_fact_denormalized.withColumnRenamed(
            "DESCRICAO", "CO_PROVA_LC"
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_tipo_prova,
            [dim_tipo_prova.CODIGO_PROVA == df_fact_denormalized.CO_PROVA_MT],
            "LEFT",
        ).drop(*["CO_PROVA_MT", "CO_PROVA", "CODIGO_PROVA"])
        df_fact_denormalized = df_fact_denormalized.withColumnRenamed(
            "DESCRICAO", "CO_PROVA_MT"
        )
        df_fact_denormalized = df_fact_denormalized.join(
            dim_tipo_prova,
            [dim_tipo_prova.CODIGO_PROVA == df_fact_denormalized.CO_PROVA_CN],
            "LEFT",
        ).drop(*["CO_PROVA", "CODIGO_PROVA", "CO_PROVA_CN"])
        df_fact_denormalized = df_fact_denormalized.withColumnRenamed(
            "DESCRICAO", "CO_PROVA_CN"
        )
        return df_fact_denormalized
