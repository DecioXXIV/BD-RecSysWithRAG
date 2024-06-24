from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import explode, col
import argparse

# Initialize Spark session
spark = SparkSession.builder.appName("ALSExample").getOrCreate()

# Define argparse to get userId and movieId from command line
parser = argparse.ArgumentParser(description="ALS Recommender Example")
parser.add_argument("--userId", type=int, help="User ID for recommendation", required=True)
parser.add_argument("--movieId", type=int, help="Movie ID for recommendation", required=True)
args = parser.parse_args()

# Read the dataset and skip the header row
lines = spark.read.text("datasets/ratings.csv").rdd
header = lines.first()
parts = lines.filter(lambda row: row != header).map(lambda row: row.value.split(","))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2])))
ratings = spark.createDataFrame(ratingsRDD)

# Build the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(ratings)

# Create a DataFrame with the specified userId and movieId
user_movie = spark.createDataFrame([(args.userId, args.movieId)], ["userId", "movieId"])

# Use the model to predict the rating
predictions = model.transform(user_movie)

# Extract the predicted rating
predicted_rating = predictions.select("prediction").collect()[0][0]

print(f"Predicted rating for user {args.userId} on movie {args.movieId}: {predicted_rating}")

# Stop the Spark session
spark.stop()
