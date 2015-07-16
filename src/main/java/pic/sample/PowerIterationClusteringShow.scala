package pic.sample

import org.apache.spark.mllib.clustering.{PowerIterationClustering, PowerIterationClusteringModel}
import org.apache.spark.{SparkConf, SparkContext}

object PowerIterationClusteringShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data = sc.textFile("data/mllib/pic_data.txt")
  val similarities = data.map(_.split(' ')match { case Array(q, w, l) => (q.toLong,w.toLong,l.toDouble)}).cache()

  val pic = new PowerIterationClustering()
    .setK(3)
    .setMaxIterations(20)
  val model = pic.run(similarities)

  model.assignments.foreach { a =>
    println(s"${a.id} -> ${a.cluster}")
  }

  // Save and load model
  model.save(sc, "pic-model")
  val sameModel = PowerIterationClusteringModel.load(sc, "pic-model")

}
