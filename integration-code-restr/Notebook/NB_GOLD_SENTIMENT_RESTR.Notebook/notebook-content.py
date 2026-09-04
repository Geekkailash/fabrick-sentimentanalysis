# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9d62234a-9748-40ca-b0a4-b189d2012483",
# META       "default_lakehouse_name": "LH_GOLD_RESTR",
# META       "default_lakehouse_workspace_id": "5fc56f33-132b-4c9b-bb8b-0402af1bd32a",
# META       "known_lakehouses": [
# META         {
# META           "id": "9d62234a-9748-40ca-b0a4-b189d2012483"
# META         },
# META         {
# META           "id": "8c8e638f-e7a4-4618-8b97-6b35af7b08f7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# gold.dim_restaurants

#This should contain one row per restaurant/outlet rather than repeating restaurant information for every review.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_clean = spark.sql("SELECT * FROM LH_SILVER_RESTR.silver.restr_review_clean")


# ---------------------------------------------------------
# 1. Create Restaurant Dimension
# ---------------------------------------------------------

dim_restaurants = (
    df_clean
    .select(
        "RestaurantChain",
        "OutletName"
    )
    .dropDuplicates()
)

window_restaurant = Window.orderBy(
    "RestaurantChain",
    "OutletName"
)

dim_restaurants = (
    dim_restaurants
    .withColumn(
        "RestaurantKey",
        F.row_number().over(window_restaurant)
    )
    .select(
        "RestaurantKey",
        "RestaurantChain",
        "OutletName"
    )
)

# Save Restaurant Dimension
(
    dim_restaurants.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_restaurants")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#--if the silve does not exist
#CREATE SCHEMA IF NOT EXISTS gold

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#gold.dim_locations
#This should represent the geographical information.
#City 
#LocationKey


dim_locations = (
    df_clean
    .select("City")
    .filter(F.col("City").isNotNull())
    .dropDuplicates()
)

window_location = Window.orderBy("City")

dim_locations = (
    dim_locations
    .withColumn(
        "LocationKey",
        F.row_number().over(window_location)
    )
    .select(
        "LocationKey",
        "City"
    )
)

# Save Location Dimension
(
    dim_locations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_locations")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#3. gold.fact_reviews
#This is your main review-level fact table.

# replaced: 
#RestaurantChain
#OutletName
#City
# with:
#RestaurantKey
#LocationKey
fact_reviews = (
    df_clean

    # Add RestaurantKey
    .join(
        dim_restaurants,
        on=["RestaurantChain", "OutletName"],
        how="left"
    )

    # Add LocationKey
    .join(
        dim_locations,
        on=["City"],
        how="left"
    )

    # Create derived attributes
    .withColumn(
        "review_month",
        F.date_format(
            F.col("ReviewDate"),
            "yyyy-MM"
        )
    )

    .withColumn(
        "review_length",
        F.when(
            F.col("ReviewText").isNull(),
            0
        ).otherwise(
            F.length(F.col("ReviewText"))
        )
    )

    .withColumn(
        "customer_rating_category",
        F.when(
            F.col("RatingScore").isNull() |
            F.col("MaxRating").isNull() |
            (F.col("MaxRating") <= 0),
            "Unknown"
        )
        .when(
            (F.col("RatingScore") / F.col("MaxRating")) >= 0.80,
            "Excellent"
        )
        .when(
            (F.col("RatingScore") / F.col("MaxRating")) >= 0.60,
            "Good"
        )
        .when(
            (F.col("RatingScore") / F.col("MaxRating")) >= 0.40,
            "Average"
        )
        .otherwise("Poor")
    )

    # Select Gold columns
    .select(
        "ReviewID",
        "RestaurantKey",
        "LocationKey",
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
        "review_month",
        "review_length",
        "customer_rating_category",
        "last_modified"
    )
)

display(fact_reviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    fact_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_reviews")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#gold.fact_review_sentiments

#API/model-generated sentiment results

#silver.restr_review_sentiment

df_sentiment = spark.sql("SELECT * FROM LH_SILVER_RESTR.silver.restr_review_sentiment")



fact_review_sentiments = (
    df_sentiment
    .select(
        "ReviewID",
        "FoodQuantity",
        "Service",
        "Cleanliness",
        "Delivery",
        "WaitingTime",
        "Pricing",
        "OverallSentiment",
        "sentiment_analyzed_at"
    )
)

(
    fact_review_sentiments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_review_sentiments")
)

display(df_sentiment)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#gold.dim_restaurants
#gold.dim_locations
#gold.fact_reviews
#gold.fact_review_sentiments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
