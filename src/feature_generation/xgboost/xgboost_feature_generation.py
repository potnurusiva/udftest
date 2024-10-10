from datetime import datetime, timedelta
from pyspark.storagelevel import StorageLevel
from itertools import groupby

from src.config_generation.config_factory import XgboostBaseConfigTemplate
from pyspark.sql import DataFrame as SparkDataFrame
from typing import Dict

import pyspark.sql.functions as sf
import pyspark.sql.types as st
from collections import defaultdict
import pandas as pd



class XgboostFeatureGenrationMock:
    def __init__(self) -> None:
        pass

    def execute(self):
        df = pd.DataFrame({"col_one":["Xgboost"], "col_two":[1]})
        return df

