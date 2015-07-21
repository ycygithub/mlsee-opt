package bayes.sample

import java.io.PrintWriter

import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)


  val data = sc.textFile("data/mllib/sample_naive_bayes_data.txt")
  val parsedData = data.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
  }
  // Split data into training (60%) and test (40%).
  val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0)
  val test = splits(1)

  val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

  val out = new PrintWriter("naive-bayes-result.txt")
  out.println("test length = " + test.collect().length)
  test.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "naive-bayes-model")
  val sameModel = NaiveBayesModel.load(sc, "naive-bayes-model")

}
