import pyspark.sql.functions as sf
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

from typing import Dict
from src.generic_utils.spark_utils import spark
import pandas as pd
from src.config_generation.config_factory import HuluBaseConfigTemplate


class HuluFeatureGeneration:
    def __init__(self) -> None:
        pass

    def execute(self):
        df = pd.DataFrame({"col_one":["Hulu"], "col_two":[1]})
        return spark.createDataFrame(df)
    
