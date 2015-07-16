package gm.sample

import java.io.PrintWriter

import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object GaussianMixtureShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data = sc.textFile("data/mllib/gmm_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()

  // Cluster the data into two classes using GaussianMixture
  val gmm = new GaussianMixture().setK(2).run(parsedData)

  // Save and load model
  gmm.save(sc, "gm-model")
  val sameModel = GaussianMixtureModel.load(sc, "gm-model")

  // output parameters of max-likelihood model
  for (i <- 0 until gmm.k) {
    println("weight=%f\nmu=%s\nsigma=\n%s\n" format
      (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
  }

  val out = new PrintWriter("gm-result.txt")
  println("clusters.k-->"+gmm.k)

  for(i <- 0 until parsedData.collect().length) {
    out.println(parsedData.collect().apply(i) +"--predict:"+gmm.predict(parsedData).collect().apply(i))
  }
  for(i <- 0 until parsedData.collect().length) {
    out.println(parsedData.collect().apply(i) +"--predictSoft:"+gmm.predictSoft(parsedData).collect().apply(i).apply(0))
  }

  out.close()

}
