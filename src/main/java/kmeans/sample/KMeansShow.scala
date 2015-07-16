package kmeans.sample

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

object KMeansShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data = sc.textFile("data/mllib/kmeans_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

  // Cluster the data into two classes using KMeans
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(parsedData, numClusters, numIterations)

  println("clusters.k-->"+clusters.k)
  println("Vector 0.1,0.1,0.1 belongs to clustering :"+clusters.predict(Vectors.dense(0.1, 0.1, 0.1)))
  println("Vector 9.2,9.2,9.2 belongs to clustering :"+clusters.predict(Vectors.dense(9.2, 9.2, 9.2)))

  // Evaluate clustering by computing Within Set Sum of Squared Errors
  val WSSSE = clusters.computeCost(parsedData)
  println("Within Set Sum of Squared Errors = " + WSSSE)

  // Save and load model
  clusters.save(sc, "k-means-model")
  val sameModel = KMeansModel.load(sc, "k-means-model")

}
