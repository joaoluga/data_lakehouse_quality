from packages.business_logic.analytics.fact_candidato_denormalized import (
    FactCandidatoDenormalized,
)
from packages.business_logic.landing.enem_extract import EnemExtract
from packages.business_logic.raw.enem_microdados import EnemMicrodadosWorkflow
from packages.business_logic.trusted.dim_candidato import DimCandidato
from packages.business_logic.trusted.dim_escola import DimEscola
from packages.business_logic.trusted.dim_status_redacao import DimStatusRedacao
from packages.business_logic.trusted.dim_tipo_prova import DimTipoProva
from packages.business_logic.trusted.fact_candidato import FactCandidato


class ETLFactory:
    etl_class = {
        "landing": {"enem_extract": EnemExtract},
        "raw": {
            "enem_microdados": EnemMicrodadosWorkflow,
        },
        "trusted": {
            "dim_candidato": DimCandidato,
            "dim_escola": DimEscola,
            "fact_candidato": FactCandidato,
            "dim_tipo_prova": DimTipoProva,
            "dim_status_redacao": DimStatusRedacao,
        },
        "analytics": {
            "fato_candidato_denormalized": FactCandidatoDenormalized,
        },
    }

    def __init__(self, entity_name, process):
        self.entity_name = entity_name
        self.process = process

    def get_etl_class(self):
        return self.etl_class[self.process][self.entity_name]
