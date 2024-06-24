from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


spark = SparkSession.builder.appName("ALSExample").getOrCreate()

lines = spark.read.text("datasets/ratings.csv").rdd
header = lines.first()
parts = lines.filter(lambda row: row != header).map(lambda row: row.value.split(","))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                     rating=float(p[2])))
ratings = spark.createDataFrame(ratingsRDD)

#(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(ratings)

# Evaluate the model by computing the RMSE on the test data
#predictions = model.transform(test)
#evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
#rmse = evaluator.evaluate(predictions)
#print("Root-mean-square error = " + str(rmse))

# Generate top 10 movie recommendations for each user
#userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
#movieRecs = model.recommendForAllItems(10)

# Generate top 10 movie recommendations for a specified set of users
#users = ratings.select(als.getUserCol()).distinct().limit(3)
users = ratings.select(als.getUserCol()).distinct().filter("userId = 24")
movies = ratings.select(als.getItemCol()).distinct()
userSubsetRecs = model.recommendForUserSubset(users, movies.count())
userSubsetRecs.show(truncate=True)

recommendations = userSubsetRecs.select("userId", explode("recommendations").alias("rec"))
recommendations = recommendations.select("userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("rating"))

# Show the transformed recommendations
recommendations.show(truncate=False)
user_movie = spark.createDataFrame([(24, 91630)], ["userId", "movieId"])

# Use the model to predict the rating
#predictions = model.transform(user_movie)

# Extract the predicted rating
#predicted_rating = predictions.select("prediction").collect()[0][0]

#print(f"Predicted rating for user 24 on movie 91630: {predicted_rating}")

# Stop the Spark session
spark.stop()
