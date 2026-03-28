Python 3.14.3 (v3.14.3:323c59a5e34, Feb  3 2026, 11:41:37) [Clang 16.0.0 (clang-1600.0.26.6)] on darwin
Enter "help" below or click "Help" above for more information.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, round, when, upper, trim
from pyspark.sql.types import DoubleType

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ============================================
# STEP 1: Read raw data from Bronze layer
# ============================================
print("Reading raw data from Bronze layer...")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "s3://healthcare-data-lake-yourname/bronze/"
)

print(f"Total records in Bronze: {df.count()}")
print("Original columns:", df.columns)

# ============================================
# STEP 2: HIPAA Compliance — Remove patient name
# ============================================
print("Removing patient name for HIPAA compliance...")
df = df.drop("Name")

# ============================================
# STEP 3: Clean column names
# (remove spaces, make lowercase)
# ============================================
print("Cleaning column names...")
for col_name in df.columns:
    new_name = col_name.lower().replace(" ", "_")
    df = df.withColumnRenamed(col_name, new_name)

print("Cleaned columns:", df.columns)

# ============================================
# STEP 4: Fix date formats
# ============================================
print("Fixing date formats...")
df = df.withColumn("date_of_admission", 
    to_date(col("date_of_admission"), "yyyy-MM-dd"))
df = df.withColumn("discharge_date", 
    to_date(col("discharge_date"), "yyyy-MM-dd"))

# ============================================
# STEP 5: Handle missing values
# ============================================
print("Handling missing values...")
... df = df.withColumn("billing_amount",
...     when(col("billing_amount").isNull(), 0.0)
...     .otherwise(col("billing_amount")))
... 
... df = df.withColumn("age",
...     when(col("age").isNull(), 0)
...     .otherwise(col("age")))
... 
... df = df.fillna("Unknown", subset=[
...     "gender", "blood_type", "medical_condition",
...     "doctor", "hospital", "insurance_provider",
...     "admission_type", "medication", "test_results"
... ])
... 
... # ============================================
... # STEP 6: Standardise text columns
... # ============================================
... print("Standardising text columns...")
... df = df.withColumn("gender", upper(trim(col("gender"))))
... df = df.withColumn("blood_type", upper(trim(col("blood_type"))))
... df = df.withColumn("test_results", upper(trim(col("test_results"))))
... 
... # ============================================
... # STEP 7: Round billing amount to 2 decimals
... # ============================================
... df = df.withColumn("billing_amount", 
...     round(col("billing_amount").cast(DoubleType()), 2))
... 
... # ============================================
... # STEP 8: Write clean data to Silver layer
... # ============================================
... print("Writing clean data to Silver layer...")
... df.write.mode("overwrite").parquet(
...     "s3://healthcare-data-lake-yourname/silver/"
... )
... 
... print(f"Successfully written {df.count()} records to Silver layer!")
... print("Bronze to Silver transformation complete!")
... 
