Python 3.14.3 (v3.14.3:323c59a5e34, Feb  3 2026, 11:41:37) [Clang 16.0.0 (clang-1600.0.26.6)] on darwin
Enter "help" below or click "Help" above for more information.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, count, round, datediff, max, min

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ============================================
# STEP 1: Read clean data from Silver layer
# ============================================
print("Reading clean data from Silver layer...")
df = spark.read.parquet(
    "s3://healthcare-data-lake-yourname/silver/"
)

print(f"Total records in Silver: {df.count()}")

# ============================================
# STEP 2: Calculate length of stay
# ============================================
print("Calculating length of stay...")
df = df.withColumn("length_of_stay_days",
    datediff(col("discharge_date"), col("date_of_admission")))

# ============================================
# STEP 3: Gold Table 1
# Average billing by medical condition
# ============================================
print("Creating Gold Table 1 - Billing by condition...")
gold_billing = df.groupBy("medical_condition").agg(
    round(avg("billing_amount"), 2).alias("avg_billing_amount"),
    count("*").alias("total_patients"),
    round(avg("length_of_stay_days"), 1).alias("avg_stay_days")
).orderBy("avg_billing_amount", ascending=False)

gold_billing.write.mode("overwrite").parquet(
    "s3://healthcare-data-lake-yourname/gold/billing_by_condition/"
)
print("Gold Table 1 complete!")

# ============================================
# STEP 4: Gold Table 2
# Patient count and revenue by hospital
# ============================================
print("Creating Gold Table 2 - Stats by hospital...")
gold_hospital = df.groupBy("hospital").agg(
    count("*").alias("total_patients"),
    round(avg("billing_amount"), 2).alias("avg_billing_amount"),
    round(avg("length_of_stay_days"), 1).alias("avg_stay_days")
).orderBy("total_patients", ascending=False)
... 
... gold_hospital.write.mode("overwrite").parquet(
...     "s3://healthcare-data-lake-yourname/gold/stats_by_hospital/"
... )
... print("Gold Table 2 complete!")
... 
... # ============================================
... # STEP 5: Gold Table 3
... # Test results distribution
... # ============================================
... print("Creating Gold Table 3 - Test results...")
... gold_test_results = df.groupBy("test_results").agg(
...     count("*").alias("total_patients"),
...     round(avg("billing_amount"), 2).alias("avg_billing_amount")
... ).orderBy("total_patients", ascending=False)
... 
... gold_test_results.write.mode("overwrite").parquet(
...     "s3://healthcare-data-lake-yourname/gold/test_results_distribution/"
... )
... print("Gold Table 3 complete!")
... 
... # ============================================
... # STEP 6: Gold Table 4
... # Admission type analysis
... # ============================================
... print("Creating Gold Table 4 - Admission type analysis...")
... gold_admission = df.groupBy("admission_type").agg(
...     count("*").alias("total_patients"),
...     round(avg("billing_amount"), 2).alias("avg_billing_amount"),
...     round(avg("length_of_stay_days"), 1).alias("avg_stay_days"),
...     round(avg("age"), 1).alias("avg_patient_age")
... ).orderBy("total_patients", ascending=False)
... 
... gold_admission.write.mode("overwrite").parquet(
...     "s3://healthcare-data-lake-yourname/gold/admission_type_analysis/"
... )
... print("Gold Table 4 complete!")
... 
... print("Silver to Gold transformation complete!")
