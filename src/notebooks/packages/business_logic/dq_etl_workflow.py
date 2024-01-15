import os
from datetime import datetime

from packages.etl.etl_factory import ETLFactory
from packages.utils.logger import Logger


class DQETLWorkflow:
    def __init__(self, spark_session, include_dq_workflow=True):
        self.spark_session = spark_session
        self.include_dq_workflow = include_dq_workflow
        self.logger = Logger()

    def extract(self):
        self.logger.info("DQETLWorkflow extract started")
        enem_extract = ETLFactory(
            entity_name="enem_extract", process="landing"
        ).get_etl_class()
        enem_extract(spark_session=self.spark_session, base_path=os.getcwd()).execute()
        self.logger.info("DQETLWorkflow extract completed")

    def generate_raw(self):
        self.logger.info("DQETLWorkflow generate_raw started")
        enem_microdados = ETLFactory(
            entity_name="enem_microdados", process="raw"
        ).get_etl_class()
        for year in [2019, 2020, 2021, 2022]:
            enem_microdados(
                spark_session=self.spark_session, base_path=os.getcwd(), year=year
            ).execute(include_dq_workflow=self.include_dq_workflow)
        self.logger.info("DQETLWorkflow generate_raw completed")

    def generate_trusted(self):
        self.logger.info("DQETLWorkflow generate_trusted started")
        dim_candidato = ETLFactory(
            entity_name="dim_candidato", process="trusted"
        ).get_etl_class()
        dim_candidato(spark_session=self.spark_session, base_path=os.getcwd()).execute(
            include_dq_workflow=self.include_dq_workflow
        )

        dim_escola = ETLFactory(
            entity_name="dim_escola", process="trusted"
        ).get_etl_class()
        dim_escola(spark_session=self.spark_session, base_path=os.getcwd()).execute(
            include_dq_workflow=self.include_dq_workflow
        )

        fact_candidato = ETLFactory(
            entity_name="fact_candidato", process="trusted"
        ).get_etl_class()
        fact_candidato(spark_session=self.spark_session, base_path=os.getcwd()).execute(
            include_dq_workflow=self.include_dq_workflow
        )

        dim_tipo_prova = ETLFactory(
            entity_name="dim_tipo_prova", process="trusted"
        ).get_etl_class()
        dim_tipo_prova(spark_session=self.spark_session, base_path=os.getcwd()).execute(
            include_dq_workflow=self.include_dq_workflow
        )

        dim_status_redacao = ETLFactory(
            entity_name="dim_status_redacao", process="trusted"
        ).get_etl_class()
        dim_status_redacao(
            spark_session=self.spark_session, base_path=os.getcwd()
        ).execute(include_dq_workflow=self.include_dq_workflow)
        self.logger.info("DQETLWorkflow generate_trusted completed")

    def generate_analytics(self):
        self.logger.info("DQETLWorkflow generate_analytics started")
        fato_candidato_denormalized = ETLFactory(
            entity_name="fato_candidato_denormalized", process="analytics"
        ).get_etl_class()
        fato_candidato_denormalized(
            spark_session=self.spark_session, base_path=os.getcwd()
        ).execute(include_dq_workflow=self.include_dq_workflow)

    def execute(self):
        self.logger.info("DQETLWorkflow started")
        # self.extract()
        start_time = datetime.now()
        print("Starting Generate Raw")
        self.generate_raw()
        print(f"Generate Raw took: {datetime.now() - start_time}")
        start_time = datetime.now()
        print("Starting Generate Trusted")
        self.generate_trusted()
        print(f"Generate Trusted took: {datetime.now() - start_time}")
        start_time = datetime.now()
        print("Starting Generate Analytics")
        self.generate_analytics()
        print(f"Generate Analytics took: {datetime.now() - start_time}")
        self.logger.info("DQETLWorkflow completed")
