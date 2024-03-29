import os

import pyspark
from packages.etl.etl_with_data_quality import ETLWithDataQuality
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def denormalization_prova(col):
    rules = {
        503: "Azul",
        504: "Amarela",
        505: "Cinza",
        506: "Rosa",
        519: "Laranja - Adaptada Ledor",
        523: "Verde - Videoprova - Libras",
        543: "Amarela (Reaplicação)",
        544: "Cinza (Reaplicação)",
        545: "Azul (Reaplicação)",
        546: "Rosa (Reaplicação)",
        507: "Azul",
        508: "Amarela",
        509: "Branca",
        510: "Rosa",
        520: "Laranja - Adaptada Ledor",
        524: "Verde - Videoprova - Libras",
        547: "Azul (Reaplicação)",
        548: "Amarelo (Reaplicação)",
        549: "Branco (Reaplicação)",
        550: "Rosa (Reaplicação)",
        564: "Laranja - Adaptada Ledor (Reaplicação)",
        511: "Azul",
        512: "Amarela",
        513: "Rosa",
        514: "Branca",
        521: "Laranja - Adaptada Ledor",
        525: "Verde - Videoprova - Libras",
        551: "Azul (Reaplicação)",
        552: "Amarelo (Reaplicação)",
        553: "Branca (Reaplicação)",
        554: "Rosa (Reaplicação)",
        565: "Laranja - Adaptada Ledor (Reaplicação)",
        515: "Azul",
        516: "Amarela",
        517: "Rosa",
        518: "Cinza",
        522: "Laranja - Adaptada Ledor",
        526: "Verde - Videoprova - Libras",
        555: "Amarela (Reaplicação)",
        556: "Cinza (Reaplicação)",
        557: "Azul (Reaplicação)",
        558: "Rosa (Reaplicação)",
        597: "Azul",
        598: "Amarela",
        599: "Cinza",
        600: "Rosa",
        601: "Rosa - Ampliada",
        602: "Rosa - Superampliada",
        604: "Laranja - Adaptada Ledor",
        605: "Verde - Videoprova - Libras",
        677: "Azul (Reaplicação)",
        678: "Amarela (Reaplicação)",
        679: "Cinza (Reaplicação)",
        680: "Rosa (Reaplicação)",
        684: "Laranja - Adaptada Ledor (Reaplicação)",
        699: "Azul (Digital)",
        700: "Amarela (Digital)",
        701: "Rosa (Digital)",
        702: "Cinza (Digital)",
        567: "Azul",
        568: "Amarela",
        569: "Branca",
        570: "Rosa",
        571: "Rosa - Ampliada",
        572: "Rosa - Superampliada",
        574: "Laranja - Adaptada Ledor",
        575: "Verde - Videoprova - Libras",
        647: "Azul (Reaplicação)",
        648: "Amarela (Reaplicação)",
        649: "Branca (Reaplicação)",
        650: "Rosa (Reaplicação)",
        654: "Laranja - Adaptada Ledor (Reaplicação)",
        687: "Azul (Digital)",
        688: "Amarela (Digital)",
        689: "Branca (Digital)",
        690: "Rosa (Digital)",
        577: "Azul",
        578: "Amarela",
        579: "Rosa",
        580: "Branca",
        581: "Rosa - Ampliada",
        582: "Rosa - Superampliada",
        584: "Laranja - Adaptada Ledor",
        585: "Verde - Videoprova - Libras",
        657: "Azul (Reaplicação)",
        658: "Amarela (Reaplicação)",
        659: "Rosa (Reaplicação)",
        660: "Branca (Reaplicação)",
        664: "Laranja - Adaptada Ledor (Reaplicação)",
        691: "Azul (Digital)",
        692: "Amarela (Digital)",
        693: "Branca (Digital)",
        694: "Rosa (Digital)",
        587: "Azul",
        588: "Amarela",
        589: "Rosa",
        590: "Cinza",
        591: "Rosa - Ampliada",
        592: "Rosa - Superampliada",
        594: "Laranja - Adaptada Ledor",
        595: "Verde - Videoprova - Libras",
        667: "Azul (Reaplicação)",
        668: "Amarela (Reaplicação)",
        669: "Rosa (Reaplicação)",
        670: "Cinza (Reaplicação)",
        674: "Laranja - Adaptada Ledor (Reaplicação)",
        695: "Azul (Digital)",
        696: "Amarela (Digital)",
        697: "Rosa (Digital)",
        698: "Cinza (Digital)",
        909: "Azul",
        910: "Amarela",
        911: "Cinza",
        912: "Rosa",
        915: "Laranja - Braille",
        916: "Laranja - Adaptada Ledor",
        917: "Verde - Videoprova - Libras",
        989: "Azul (Reaplicação)",
        990: "Amarela (Reaplicação)",
        991: "Cinza (Reaplicação)",
        992: "Rosa (Reaplicação)",
        996: "Laranja - Adaptada Ledor (Reaplicação)",
        1011: "Azul (Digital)",
        1012: "Amarela (Digital)",
        1013: "Rosa (Digital)",
        1014: "Cinza (Digital)",
        1045: "Azul (Segunda oportunidade)",
        1046: "Amarela (Segunda oportunidade)",
        1047: "Cinza (Segunda oportunidade)",
        1048: "Rosa (Segunda oportunidade)",
        1052: "Laranja - Adaptada Ledor (Segunda oportunidade)",
        879: "Azul",
        880: "Amarela",
        881: "Branca",
        882: "Rosa",
        885: "Laranja - Braille",
        886: "Laranja - Adaptada Ledor",
        887: "Verde - Videoprova - Libras",
        959: "Azul (Reaplicação)",
        960: "Amarela (Reaplicação)",
        961: "Branca (Reaplicação)",
        962: "Rosa (Reaplicação)",
        966: "Laranja - Adaptada Ledor (Reaplicação)",
        999: "Azul (Digital)",
        1000: "Amarela (Digital)",
        1001: "Branca (Digital)",
        1002: "Rosa (Digital)",
        1015: "Azul (Segunda oportunidade)",
        1016: "Amarela (Segunda oportunidade)",
        1017: "Branca (Segunda oportunidade)",
        1018: "Rosa (Segunda oportunidade)",
        1022: "Laranja - Adaptada Ledor (Segunda oportunidade)",
        889: "Azul",
        890: "Amarela",
        891: "Rosa",
        892: "Branca",
        895: "Laranja - Braille",
        896: "Laranja - Adaptada Ledor",
        897: "Verde - Videoprova - Libras",
        969: "Azul (Reaplicação)",
        970: "Amarela (Reaplicação)",
        971: "Rosa (Reaplicação)",
        972: "Branca (Reaplicação)",
        976: "Laranja - Adaptada Ledor (Reaplicação)",
        1003: "Azul (Digital)",
        1004: "Amarela (Digital)",
        1005: "Branca (Digital)",
        1006: "Rosa (Digital)",
        1025: "Azul (Segunda oportunidade)",
        1026: "Amarela (Segunda oportunidade)",
        1027: "Rosa (Segunda oportunidade)",
        1028: "Branca (Segunda oportunidade)",
        1032: "Laranja - Adaptada Ledor (Segunda oportunidade)",
        899: "Azul",
        900: "Amarela",
        901: "Rosa",
        902: "Cinza",
        905: "Laranja - Braille",
        906: "Laranja - Adaptada Ledor",
        907: "Verde - Videoprova - Libras",
        979: "Azul (Reaplicação)",
        980: "Amarela (Reaplicação)",
        981: "Rosa (Reaplicação)",
        982: "Cinza (Reaplicação)",
        986: "Laranja - Adaptada Ledor (Reaplicação)",
        1007: "Azul (Digital)",
        1008: "Amarela (Digital)",
        1009: "Rosa (Digital)",
        1010: "Cinza (Digital)",
        1035: "Azul (Segunda oportunidade)",
        1036: "Amarela (Segunda oportunidade)",
        1037: "Cinza (Segunda oportunidade)",
        1038: "Rosa (Segunda oportunidade)",
        1042: "Laranja - Adaptada Ledor (Segunda oportunidade)",
        1085: "Azul",
        1086: "Amarela",
        1087: "Cinza",
        1088: "Rosa",
        1092: "Laranja - Adaptada Ledor",
        1093: "Verde - Videoprova - Libras",
        1165: "Azul (Reaplicação)",
        1166: "Amarela (Reaplicação)",
        1167: "Cinza (Reaplicação)",
        1168: "Rosa (Reaplicação)",
        1187: "Azul (Digital)",
        1188: "Amarela (Digital)",
        1189: "Rosa (Digital)",
        1190: "Cinza (Digital)",
        1055: "Azul",
        1056: "Amarela",
        1057: "Branca",
        1058: "Rosa",
        1062: "Laranja - Adaptada Ledor",
        1063: "Verde - Videoprova - Libras",
        1135: "Azul (Reaplicação)",
        1136: "Amarela (Reaplicação)",
        1137: "Branca (Reaplicação)",
        1138: "Rosa (Reaplicação)",
        1175: "Azul (Digital)",
        1176: "Amarela (Digital)",
        1177: "Branca (Digital)",
        1178: "Rosa (Digital)",
        1065: "Azul",
        1066: "Amarela",
        1067: "Rosa",
        1068: "Branca",
        1072: "Laranja - Adaptada Ledor",
        1073: "Verde - Videoprova - Libras",
        1145: "Azul (Reaplicação)",
        1146: "Amarela (Reaplicação)",
        1147: "Rosa (Reaplicação)",
        1148: "Branca (Reaplicação)",
        1179: "Azul (Digital)",
        1180: "Amarela (Digital)",
        1181: "Branca (Digital)",
        1182: "Rosa (Digital)",
        1075: "Azul",
        1076: "Amarela",
        1077: "Rosa",
        1078: "Cinza",
        1082: "Laranja - Adaptada Ledor",
        1083: "Verde - Videoprova - Libras",
        1155: "Azul (Reaplicação)",
        1156: "Amarela (Reaplicação)",
        1157: "Rosa (Reaplicação)",
        1158: "Cinza (Reaplicação)",
        1183: "Azul (Digital)",
        1184: "Amarela (Digital)",
        1185: "Rosa (Digital)",
        1186: "Cinza (Digital)",
        None: None,
    }
    return rules[col]


class DimTipoProva(ETLWithDataQuality):
    partitions = []
    mode = "upsert"
    merge_logic = "t.CODIGO_PROVA = s.CODIGO_PROVA"
    update_logic = "t.ingestion_at <= s.ingestion_at"

    target_layer = "trusted"

    table_name = "dim_tipo_prova"

    dataframe_view_name = "dim_tipo_prova_df"

    primary_key = "CODIGO_PROVA"

    checks = f"""checks for {dataframe_view_name}:
- row_count > 0
- missing_count(CO_PROVA) = 0
- invalid_count(CO_PROVA) = 0:
    valid values: ['CO_PROVA_MT', 'CO_PROVA_CN', 'CO_PROVA_CH', 'CO_PROVA_LC']
- missing_count(CODIGO_PROVA) = 0
- duplicate_count(CODIGO_PROVA) = 0
- invalid_count(CODIGO_PROVA) = 0:
    valid min: '503'
- invalid_count(CODIGO_PROVA) = 0:
    valid max: '1190'
- missing_count(DESCRICAO) = 0
- invalid_count(DESCRICAO) = 0:
    valid values: ['Amarela',
 'Amarela (Digital)',
 'Amarela (Reaplicação)',
 'Amarela (Segunda oportunidade)',
 'Amarelo (Reaplicação)',
 'Azul',
 'Azul (Digital)',
 'Azul (Reaplicação)',
 'Azul (Segunda oportunidade)',
 'Branca',
 'Branca (Digital)',
 'Branca (Reaplicação)',
 'Branca (Segunda oportunidade)',
 'Branco (Reaplicação)',
 'Cinza',
 'Cinza (Digital)',
 'Cinza (Reaplicação)',
 'Cinza (Segunda oportunidade)',
 'Laranja - Adaptada Ledor',
 'Laranja - Adaptada Ledor (Reaplicação)',
 'Laranja - Adaptada Ledor (Segunda oportunidade)',
 'Laranja - Braille',
 'Rosa',
 'Rosa (Digital)',
 'Rosa (Reaplicação)',
 'Rosa (Segunda oportunidade)',
 'Rosa - Ampliada',
 'Rosa - Superampliada',
 'Verde - Videoprova - Libras']
    """

    post_check = None

    def data_transform_workflow(self) -> pyspark.sql.DataFrame:
        df = self.spark_session.read.format("delta").load(
            (os.path.join(os.getcwd(), "layers", "raw", "tables", "enem_microdados"))
        )
        df = df.selectExpr("CO_PROVA_CN", "CO_PROVA_CH", "CO_PROVA_LC", "CO_PROVA_MT")
        stacked_df = df.selectExpr(
            "stack(4, 'CO_PROVA_CN', CO_PROVA_CN, 'CO_PROVA_CH', CO_PROVA_CH, 'CO_PROVA_LC', CO_PROVA_LC, 'CO_PROVA_MT', CO_PROVA_MT) as (CO_PROVA, CODIGO_PROVA)"
        )
        stacked_df = stacked_df.withColumn(
            f"DESCRICAO", denormalization_prova(stacked_df["CODIGO_PROVA"])
        )
        stacked_df = stacked_df.drop_duplicates()
        return stacked_df
