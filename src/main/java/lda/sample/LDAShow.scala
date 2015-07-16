package lda.sample

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object LDAShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data = sc.textFile("data/mllib/sample_lda_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))

  // Index documents with unique IDs
  val corpus = parsedData.zipWithIndex.map(_.swap).cache()

  // Cluster the documents into three topics using LDA
  val ldaModel = new LDA().setK(3).run(corpus)

  // Output topics. Each is a distribution over words (matching word count vectors)
  println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")

  val topics = ldaModel.topicsMatrix
  println("ldaModel.k-->"+ldaModel.k)

  for (topic <- Range(0, 3)) {
    print("Topic " + topic + ":")
    for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
    println()
  }

}
