import io
import os
import ssl
import time
import zipfile

import requests
import urllib3
from packages.utils.logger import Logger


class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session


class EnemExtract:
    def __init__(self, spark_session, base_path):
        self._logger = Logger()
        self.spark_session = spark_session
        self.base_path = base_path

    def execute(self):
        for year in [2019, 2020, 2021, 2022]:
            url_zip = (
                f"https://download.inep.gov.br/microdados/microdados_enem_{year}.zip"
            )

            max_retries = 3  # Número máximo de tentativas

            for retry in range(max_retries):
                try:
                    self._logger.info(f"Tentativa {retry + 1} - Iniciando o download")

                    response = get_legacy_session().get(url_zip)
                    self._logger.info(f"Tentativa {retry + 1} - Download concluído")

                    if response.status_code == 200:
                        zip_bytes = response.content

                        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                            for file_info in zip_file.infolist():
                                arquivo_nome = file_info.filename
                                if arquivo_nome not in (
                                    "DADOS/",
                                    f"DADOS/MICRODADOS_ENEM_{year}.csv",
                                ):
                                    continue
                                else:
                                    arquivo_nome_decoded = file_info.filename.encode(
                                        "cp437"
                                    ).decode("utf-8", "ignore")
                                    self._logger.info(
                                        f"Salvando o arquivo {arquivo_nome_decoded}"
                                    )

                                    arquivo_bytes = zip_file.read(arquivo_nome)

                                    # Constrói o caminho completo do arquivo de saída
                                    item_path = os.path.join(
                                        os.getcwd(),
                                        "layers",
                                        "landing",
                                        "enem",
                                        year,
                                        arquivo_nome_decoded,
                                    )

                                    if file_info.is_dir():
                                        if not os.path.exists(item_path):
                                            os.makedirs(item_path)
                                    else:
                                        with zip_file.open(file_info) as source, open(
                                            item_path, "wb"
                                        ) as target:
                                            target.write(source.read())

                        self._logger.info("Extração e salvamento concluídos")

                    else:
                        self._logger.error(
                            f"Falha na tentativa {retry + 1} - Falha ao baixar o arquivo ZIP da URL."
                        )
                except Exception as e:
                    self._logger.error(
                        f"Tentativa {retry + 1} - Ocorreu um erro: {str(e)}"
                    )

                if retry < max_retries - 1:
                    time.sleep(5)
