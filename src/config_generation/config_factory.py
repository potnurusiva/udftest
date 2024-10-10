# config executor

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import pandas as pd
from src.generic_utils.spark_utils import dbutils, spark

@dataclass
class BaseConfig:
    runDate: str
    runEnv: str
    subEnv: str
    caller: str
    service: str


class CreateDefaultBaseConfig:
    def __init__(self):
        ...

    def execute(self):
        dbutils.widgets.removeAll()

        dbutils.widgets.text("SERVICE", "DISNEY")  # HULU XGBOOST

        dbutils.widgets.text("DATE", "")
        dbutils.widgets.text("ENV", "prod")  # prod
        dbutils.widgets.text("SUB_ENV", "prod")  # prod
        dbutils.widgets.text("CALLER", "databricks")  # databricks

        base_config = BaseConfig(
            runDate=date.today().strftime("%Y-%m-%d")
            if not dbutils.widgets.get("DATE")
            else dbutils.widgets.get("DATE"),
            runEnv=dbutils.widgets.get("ENV"),
            subEnv=dbutils.widgets.get("SUB_ENV"),
            caller=dbutils.widgets.get("CALLER"),
            service=dbutils.widgets.get("SERVICE"),
        )
        return base_config


@dataclass
class DisneyBaseConfigTemplate:
    base_config: BaseConfig
 

@dataclass
class HuluBaseConfigTemplate:
    base_config: BaseConfig


@dataclass
class XgboostBaseConfigTemplate:
    base_config: BaseConfig


class DisneyConfig:
    def __init__(self):
        ...

    def execute(self, base_config: BaseConfig, **kwargs) -> DisneyBaseConfigTemplate:
        spark.conf.set("spark.databricks.io.cache.enabled", "true")
        spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        return DisneyBaseConfigTemplate(base_config=base_config)


class HuluConfig:
    def __init__(self):
        ...

    def execute(self, base_config: BaseConfig, **kwargs) -> HuluBaseConfigTemplate:
        return HuluBaseConfigTemplate(base_config=base_config)


class XgboostConfig:
    def __init__(self):
        ...

    def execute(self, base_config: BaseConfig, **kwargs) -> XgboostBaseConfigTemplate:
        spark.conf.set("spark.databricks.io.cache.enabled", "true")
        spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        return XgboostBaseConfigTemplate(base_config=base_config)


class ConfigExecutor:
    config_map = {
        "DISNEY": DisneyConfig(),
        "HULU": HuluConfig(),
        "XGBOOST": XgboostConfig(),
    }

    def __init__(self, base_config: BaseConfig):
        self.base_config = base_config

    def execute(self) -> DisneyConfig | HuluConfig | XgboostConfig:

        if self.base_config.service not in self.config_map.keys():
            raise KeyError(f"{self.base_config.service} not in the config map")

        return self.config_map[self.base_config.service].execute(
            base_config=self.base_config
        )

