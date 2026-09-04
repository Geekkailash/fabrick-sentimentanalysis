# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8c8e638f-e7a4-4618-8b97-6b35af7b08f7",
# META       "default_lakehouse_name": "LH_SILVER_RESTR",
# META       "default_lakehouse_workspace_id": "5fc56f33-132b-4c9b-bb8b-0402af1bd32a",
# META       "known_lakehouses": [
# META         {
# META           "id": "8c8e638f-e7a4-4618-8b97-6b35af7b08f7"
# META         },
# META         {
# META           "id": "b1f4b002-b5e1-4d34-930b-6fa04b3d9313"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": [
# META         {
# META           "id": "6cd24f06-91c0-49d3-96b8-658a6c4f1c5e",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!



from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, DecimalType, DateType, TimestampType
)

# Read bronze/raw Delta table
df_raw =  spark.sql("SELECT * FROM LH_BRONZE_RESTR.dbo.d_one_restaurant_reviews LIMIT 1000")
display(df_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

string_columns = [
    "ReviewID",
    "RestaurantChain",
    "OutletName",
    "City",
    "Platform",
    "ReviewDate",
    "CustomerID",
    "RatingScore",
    "MaxRating",
    "PriceBand",
    "EstimatedSpendINR",
    "Cuisine",
    "ReviewTopic",
    "ReviewText",
    "VerifiedReview",
    "HelpfulVotes",
    "ResponseFromRestaurant",
    "Sentiment",
    "last_modified"
]

df_clean = df_raw

for c in string_columns:
    df_clean = df_clean.withColumn(
        c,
        F.when(
            F.trim(F.col(c)) == "",
            None
        ).otherwise(F.trim(F.col(c)))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_typed = (
    df_clean
    .withColumn(
        "ReviewDate",
        F.to_timestamp(
            F.substring(F.col("ReviewDate"), 1, 19),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn("RatingScore", F.col("RatingScore").cast("int"))
    .withColumn("MaxRating", F.col("MaxRating").cast("int"))
    .withColumn(
        "EstimatedSpendINR",
        F.regexp_replace(
            F.col("EstimatedSpendINR"),
            ",",
            ""
        ).cast("decimal(12,2)")
    )
    .withColumn("VerifiedReview", F.col("VerifiedReview").cast("int"))
    .withColumn("HelpfulVotes", F.col("HelpfulVotes").cast("int"))
    .withColumn(
        "ResponseFromRestaurant",
        F.col("ResponseFromRestaurant").cast("int")
    )
    .withColumn(
        "last_modified",
        F.to_timestamp(F.col("last_modified"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#4. Null/Data Quality checks

mandatory_columns = [
    "ReviewID",
    "RestaurantChain",
    "OutletName",
    "City",
    "Platform",
    "ReviewDate",
    "CustomerID",
    "RatingScore",
    "MaxRating",
    "EstimatedSpendINR",
    "Sentiment"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_condition = None

for c in mandatory_columns:
    condition = F.col(c).isNull()

    if null_condition is None:
        null_condition = condition
    else:
        null_condition = null_condition | condition

df_quality = df_typed.withColumn(
    "dq_null_failure",
    null_condition
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Rating should be 1 and between maximum rating allowed
df_quality = df_quality.withColumn(
    "dq_rating_failure",
    (
        F.col("RatingScore").isNull() |
        F.col("MaxRating").isNull() |
        (F.col("RatingScore") < 1) |
        (F.col("RatingScore") > F.col("MaxRating"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Rating must be positive
df_quality = df_quality.withColumn(
    "dq_max_rating_failure",
    F.col("MaxRating").isNull() |
    (F.col("MaxRating") <= 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Rating can not be negative
df_quality = df_quality.withColumn(
    "dq_spend_failure",
    F.col("EstimatedSpendINR").isNull() |
    (F.col("EstimatedSpendINR") < 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Verified review must be 0 either or 1
df_quality = df_quality.withColumn(
    "dq_verified_failure",
    F.col("VerifiedReview").isNull() |
    (~F.col("VerifiedReview").isin(0, 1))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Helopful votes can not be negative
df_quality = df_quality.withColumn(
    "dq_helpful_votes_failure",
    F.col("HelpfulVotes").isNull() |
    (F.col("HelpfulVotes") < 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Sentiment validation 
valid_sentiments = [
    "Positive",
    "Negative",
    "Neutral"
]

df_quality = df_quality.withColumn(
    "dq_sentiment_failure",
    F.col("Sentiment").isNull() |
    (~F.lower(F.col("Sentiment")).isin(
        [x.lower() for x in valid_sentiments]
    ))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Duplicate check
duplicate_ids = (
    df_typed
    .groupBy("ReviewID")
    .count()
    .filter(F.col("count") > 1)
    .select("ReviewID")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Mark duplicated

df_quality = df_quality.join(
    duplicate_ids.withColumn("dq_duplicate_failure", F.lit(True)),
    on="ReviewID",
    how="left"
).withColumn(
    "dq_duplicate_failure",
    F.coalesce(
        F.col("dq_duplicate_failure"),
        F.lit(False)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create overall data quality flag
df_quality = df_quality.withColumn(
    "dq_failure",
    (
        F.col("dq_null_failure") |
        F.col("dq_rating_failure") |
        F.col("dq_max_rating_failure") |
        F.col("dq_spend_failure") |
        F.col("dq_verified_failure") |
        F.col("dq_helpful_votes_failure") |
        F.col("dq_sentiment_failure") |
        F.col("dq_duplicate_failure")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#See how many records failed
df_quality.groupBy("dq_failure").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Remove invalid rows
df_silver = (
    df_quality
    .filter(F.col("dq_failure") == False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Remove temporay invalid columns
df_silver = (
    df_quality
    .filter(F.col("dq_failure") == False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Normalize a few values
df_silver = df_silver.withColumn(
    "Sentiment",
    F.initcap(F.trim(F.col("Sentiment")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Make sure final column order must be followd
final_columns = [
    "ReviewID",
    "RestaurantChain",
    "OutletName",
    "City",
    "Platform",
    "ReviewDate",
    "CustomerID",
    "RatingScore",
    "MaxRating",
    "PriceBand",
    "EstimatedSpendINR",
    "Cuisine",
    "ReviewTopic",
    "ReviewText",
    "VerifiedReview",
    "HelpfulVotes",
    "ResponseFromRestaurant",
    "Sentiment",
    "last_modified"
]

df_silver = df_silver.select(final_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_silver.select(final_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to silver data table
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.restr_review_clean")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#--if the silve does not exist
#--CREATE SCHEMA IF NOT EXISTS silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#
#    df_silver
#    .write
#    .format("delta")
#    .mode("overwrite")
#    .option("overwriteSchema", "true")
#    .saveAsTable("silver.restr_review_clean")
#)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
