package logistic.sample

import java.io.PrintWriter

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegressionShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load training data in LIBSVM format.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

  // Split data into training (60%) and test (40%).
  val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  // Run training algorithm to build the model
  val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(10)
    .run(training)

  // Compute raw scores on the test set.
  val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
    val prediction = model.predict(features)
    (prediction, label)
  }

  // Get evaluation metrics.
  val metrics = new MulticlassMetrics(predictionAndLabels)
  val precision = metrics.precision
  println("Precision = " + precision)


  val out = new PrintWriter("logistic-result.txt")
  out.println("test length = " + test.collect().length)
  test.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "logistic-model")
  val sameModel = LogisticRegressionModel.load(sc, "logistic-model")

}
