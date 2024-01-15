import re
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
import yaml


class SodaChecksGenerator:
    def __init__(self, df):
        self.df = df

    def check_null(self, field):
        # Check if the field is null
        has_null = self.df.where(self.df[field].isNull()).count() > 0
        # Check total of null values
        null_count = self.df.where(self.df[field].isNull()).count()
        return has_null, null_count

    def check_unique(self, field):
        # Check if the field is unique
        has_unique = self.df.select(field).distinct().count() == self.df.count()
        # Check total of unique values
        unique_count = self.df.select(field).distinct().count()
        return has_unique, unique_count

    def get_all_unique_values(self, field):
        # If the field is not unique, create a list with the unique values
        unique_values_collect = self.df.select(field).distinct().collect()
        unique_values_list = [values[0] for values in unique_values_collect]
        return unique_values_list

    def get_min_max(df, field):
        min_value = df.select(field).agg({field: "min"}).collect()[0][0]
        max_value = df.select(field).agg({field: "max"}).collect()[0][0]
        return min_value, max_value

    def profilling_generator(self):
        profilling = {}
        for field in self.df.schema:
            unique_values = None
            if field.dataType in [LongType(), IntegerType(), DoubleType()]:
                has_null, null_count = self.check_null(field.name)
                has_unique, unique_count = self.check_unique(field.name)
                min_value, max_value = self.get_min_max(field.name)
                if not has_unique:
                    unique_values = self.get_all_unique_values(field.name)
            if field.dataType == StringType():
                has_null, null_count = None, None
                min_value, max_value = None, None
                has_unique, unique_count = self.check_unique(field.name)
                if not has_unique:
                    unique_values = self.get_all_unique_values(field.name)

            if unique_values:
                if len(unique_values) >= 30:
                    unique_values = unique_values[:30]

            profilling[field.name] = {
                "type": field.dataType,
                "has_null": has_null,
                "null_count": null_count,
                "has_unique": has_unique,
                "unique_count": unique_count,
                "min_value": min_value,
                "max_value": max_value,
                "unique_values": unique_values,
            }

        return profilling

    def json_to_yaml(self, json_str):
        try:
            yaml_str = yaml.dump(json_str, default_flow_style=False, allow_unicode=True)
            yaml_str = re.sub(
                r"\\u[0-9a-fA-F]{4}",
                lambda x: bytes(x.group(), "utf-8").decode("unicode-escape"),
                yaml_str,
            )
            return yaml_str
        except Exception as e:
            return f"Failed converting JSON to YAML: {str(e)}"

    def generate_json_checks(self, profilling_dict):
        json_checks = {}
        json_checks["checks for {dataframe_view_name}"] = []
        for field in profilling_dict.keys():
            json_checks["checks for {dataframe_view_name}"].append(
                f"missing_count({field}) = 0"
            )
            if profilling_dict[field]["has_unique"] == True:
                json_checks["checks for {dataframe_view_name}"].append(
                    f"duplicate_count({field}) = 0"
                )
            if profilling_dict[field]["type"] == StringType():
                if profilling_dict[field]["unique_values"]:
                    json_checks["checks for {dataframe_view_name}"].append(
                        {
                            f"invalid_count({field}) = 0": {
                                "valid values": f"{profilling_dict[field]['unique_values']}"
                            }
                        }
                    )

            if (
                profilling_dict[field]["type"] == IntegerType()
                or profilling_dict[field]["type"] == DoubleType()
            ):
                if profilling_dict[field]["min_value"] >= 0:
                    json_checks["checks for {dataframe_view_name}"].append(
                        {
                            f"invalid_count({field}) = 0": {
                                "valid min": f"{profilling_dict[field]['min_value']}"
                            }
                        }
                    )

                if profilling_dict[field]["max_value"]:
                    json_checks["checks for {dataframe_view_name}"].append(
                        {
                            f"invalid_count({field}) = 0": {
                                "valid max": f"{profilling_dict[field]['max_value']}"
                            }
                        }
                    )
                if profilling_dict[field]["unique_values"]:
                    if len(profilling_dict[field]["unique_values"]) < 15:
                        json_checks["checks for {dataframe_view_name}"].append(
                            {
                                f"invalid_count({field}) = 0": {
                                    "valid values": f"{profilling_dict[field]['unique_values']}"
                                }
                            }
                        )
        return json_checks

    def get_soda_checks(self, profilling_dict):
        yaml_checks = self.json_to_yaml(self.generate_json_checks(profilling_dict))
        result = re.sub(r"'\[", "[", yaml_checks)
        result = re.sub(r"]'", "]", result)
        result = re.sub(r"''", "'", result)
        result = re.sub(r"None,", "", result)
        print(result)
