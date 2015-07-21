package svm.sample

import java.io.PrintWriter

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object SVMShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load training data in LIBSVM format.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

  // Split data into training (60%) and test (40%).
  val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0).cache()
  val test = splits(1)

  // Run training algorithm to build the model
  val numIterations = 100
  val model = SVMWithSGD.train(training, numIterations)

  // Clear the default threshold.
  model.clearThreshold()

  // Compute raw scores on the test set.
  val scoreAndLabels = test.map { point =>
    val score = model.predict(point.features)
    (score, point.label)
  }

  // Get evaluation metrics.
  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
  val auROC = metrics.areaUnderROC()
  println("Area under ROC = " + auROC)

  val out = new PrintWriter("svm-result.txt")
  out.println("test length = " + test.collect().length)
  test.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "svm-model")
  val sameModel = SVMModel.load(sc, "svm-model")

}
