# Databricks notebook source
import sys
import os

# COMMAND ----------

os.chdir("../..")

# COMMAND ----------

from src.config_generation.config_factory import ConfigExecutor, CreateDefaultBaseConfig
from src.feature_generation.feature_generation_factory import FeatureGenerationExecutor

# COMMAND ----------

base_conifg = CreateDefaultBaseConfig().execute()
base_conifg

# COMMAND ----------

service_config = ConfigExecutor(base_config=base_conifg).execute()
service_config

# COMMAND ----------

service_config.base_config.service

# COMMAND ----------

FeatureGenerationExecutor(service_config=service_config).execute()

# COMMAND ----------

# MAGIC %md The exact same transformations done in one-go via a notebook 

# COMMAND ----------

import pyspark.sql.functions as sf
import pyspark.sql.types as st
import hashlib


def to_hex(byte_array: str) -> str:
    high_4_bits = 0xF0
    low_4_bits = 0x0F
    bit_shift = 4
    hex_string = "".join(
        [
            format((b & high_4_bits) >> bit_shift, "x") + format(b & low_4_bits, "x")
            for b in byte_array
        ]
    )
    return hex_string

#@sf.udf(returnType=st.StringType())
def sha1HashUdf(string_input: str) -> str:
    if string_input is None or string_input == "":
        return ""
    else:
        md = hashlib.sha1()
        md.update(string_input.encode("utf-8"))
        return to_hex(md.digest())


sha1HashUdf = sf.udf(sha1HashUdf, st.StringType())

# COMMAND ----------

data = [("example",), (None,), ("",)]
df = spark.createDataFrame(data, ["input_string"])
df = df.withColumn("sha1_hash", sha1HashUdf("input_string"))

df.show()

# COMMAND ----------


