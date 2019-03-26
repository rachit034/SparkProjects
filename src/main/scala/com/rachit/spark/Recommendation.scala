package com.rachit.spark
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Recommender_Example{
  
  def main(args: Array[String]){
    
 
    
    val spark = SparkSession
    .builder()
    .appName("Recommendation")
    .master("local[*]")
    .getOrCreate()
    import spark.implicits._
    
    setupLogging()
    val ratings = spark.read.option("header","true").option("inferSchema","true").csv("../movie_ratings.csv")
    
    println(ratings.head())
    ratings.printSchema()
    
    
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    
    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
    
    // Evaluate the model by computing the average error from real rating
    val predictions = model.transform(test)
    predictions.show()
    // import to use abs()
    import org.apache.spark.sql.functions._
    val error = predictions.select(abs($"rating"-$"prediction"))
    
    // Drop NaNs
    error.na.drop().describe().show()
    
  }
  
}
    
    
    
    
  
