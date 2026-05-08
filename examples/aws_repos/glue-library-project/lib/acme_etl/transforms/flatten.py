"""
Flatten nested JSON/struct/array data from APIs and event streams.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType


def flatten_nested(df: DataFrame, max_depth: int = 5, separator: str = "_") -> DataFrame:
    """Recursively flatten all nested struct columns."""
    for _ in range(max_depth):
        has_struct = False
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                has_struct = True
                for nested_field in field.dataType.fields:
                    new_name = f"{field.name}{separator}{nested_field.name}"
                    df = df.withColumn(new_name, F.col(f"{field.name}.{nested_field.name}"))
                df = df.drop(field.name)
        if not has_struct:
            break
    return df


def explode_arrays(df: DataFrame, array_cols: list, 
                   keep_nulls: bool = True) -> DataFrame:
    """Explode specified array columns (with or without nulls)."""
    for col in array_cols:
        if keep_nulls:
            df = df.withColumn(col, F.explode_outer(F.col(col)))
        else:
            df = df.withColumn(col, F.explode(F.col(col)))
    return df


def parse_json_column(df: DataFrame, json_col: str, 
                      schema: str = None) -> DataFrame:
    """Parse a JSON string column into struct using schema inference or provided schema."""
    if schema:
        from pyspark.sql.types import _parse_datatype_string
        parsed_schema = _parse_datatype_string(schema)
        return df.withColumn(f"{json_col}_parsed", F.from_json(F.col(json_col), parsed_schema))
    else:
        # Infer schema from sample
        sample = df.select(json_col).limit(100).collect()
        json_strings = [row[0] for row in sample if row[0]]
        if json_strings:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            schema_df = spark.read.json(spark.sparkContext.parallelize(json_strings))
            inferred_schema = schema_df.schema
            return df.withColumn(f"{json_col}_parsed", F.from_json(F.col(json_col), inferred_schema))
    return df
