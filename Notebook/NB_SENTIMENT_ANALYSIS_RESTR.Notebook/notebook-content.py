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
# META           "id": "b1f4b002-b5e1-4d34-930b-6fa04b3d9313"
# META         },
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

%pip install -q transformers torch sentencepiece

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load clean data
from pyspark.sql import functions as F

df_reviews = spark.table("silver.restr_review_clean")

df_reviews = (
    df_reviews
    .select("ReviewID", "ReviewText")
    .filter(
        F.col("ReviewID").isNotNull() &
        F.col("ReviewText").isNotNull() &
        (F.trim(F.col("ReviewText")) != "")
    )
)

display(df_reviews.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load ABSA Model

from transformers import pipeline

absa_model = pipeline(
    "text-classification",
    model="yangheng/deberta-v3-base-absa-v1.1"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Testing it
review = """
Outstanding food experience at D-one. The fast food was fresh.
Overall, the experience was rated 5 out of 5.
"""

print(absa_model(review, text_pair="food"))
print(absa_model(review, text_pair="service"))
print(absa_model(review, text_pair="cleanliness"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ASPECTS = {
    "FoodQuantity": "food quantity",
    "Service": "service",
    "Cleanliness": "cleanliness",
    "Delivery": "delivery",
    "WaitingTime": "waiting time",
    "Pricing": "price"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create a function for one review ID
def analyze_review(review_text):

    result = {}

    for column_name, aspect in ASPECTS.items():

        try:

            scores = absa_model(
                review_text,
                text_pair=aspect
            )

            # Get highest probability sentiment
            best = max(
                scores,
                key=lambda x: x["score"]
            )

            result[column_name] = best["label"].capitalize()

        except Exception as e:

            result[column_name] = "Neutral"

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Test it in sample
test_result = analyze_review(review)

print(test_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = df_reviews.collect()

results = []

for row in reviews:

    review_id = row["ReviewID"]
    review_text = row["ReviewText"]

    sentiment = analyze_review(review_text)

    results.append({
        "ReviewID": review_id,
        "FoodQuantity": sentiment["FoodQuantity"],
        "Service": sentiment["Service"],
        "Cleanliness": sentiment["Cleanliness"],
        "Delivery": sentiment["Delivery"],
        "WaitingTime": sentiment["WaitingTime"],
        "Pricing": sentiment["Pricing"]
    })

print("Total reviews processed:", len(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Convert result to spark dataframe
df_sentiment = spark.createDataFrame(results)

display(df_sentiment)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add overall sentiment
def get_overall_sentiment(review_text):

    try:

        scores = absa_model(
            review_text,
            text_pair="overall restaurant experience"
        )

        best = max(
            scores,
            key=lambda x: x["score"]
        )

        return best["label"].capitalize()

    except Exception:

        return "Neutral"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Modify the processing
results = []

for row in reviews:

    review_id = row["ReviewID"]
    review_text = row["ReviewText"]

    sentiment = analyze_review(review_text)

    overall = get_overall_sentiment(review_text)

    results.append({
        "ReviewID": review_id,
        "FoodQuantity": sentiment["FoodQuantity"],
        "Service": sentiment["Service"],
        "Cleanliness": sentiment["Cleanliness"],
        "Delivery": sentiment["Delivery"],
        "WaitingTime": sentiment["WaitingTime"],
        "Pricing": sentiment["Pricing"],
        "OverallSentiment": overall
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add analysis timestamp
df_sentiment = (
    spark.createDataFrame(results)
    .withColumn(
        "sentiment_analyzed_at",
        F.current_timestamp()
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Save silver table

(
    df_sentiment
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.restr_review_sentiment")
)

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
