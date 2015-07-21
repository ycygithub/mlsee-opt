package word2vec.sample

import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.{SparkContext, SparkConf}

object Word2VecShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // This example uses text8 file from http://mattmahoney.net/dc/text8.zip
  // The file was downloadded, unziped and split into multiple lines using
  // file unziped --> data/text8/text8
  val input = sc.textFile("data/text8/text8").map(line => line.split(" ").toSeq)
  val word2vec = new Word2Vec()
  val model = word2vec.fit(input)

  val synonyms = model.findSynonyms("china", 40)
  for((synonym, cosineSimilarity) <- synonyms) {
    println(s"$synonym $cosineSimilarity")
  }

  // Save and load model
  model.save(sc, "word2vec-model")
  val sameModel = Word2VecModel.load(sc, "word2vec-model")

}
