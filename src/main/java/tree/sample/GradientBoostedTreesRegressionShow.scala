package tree.sample

import java.io.PrintWriter

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object GradientBoostedTreesRegressionShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data file.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a GradientBoostedTrees model.
  //  The defaultParams for Regression use SquaredError by default.
  val boostingStrategy = BoostingStrategy.defaultParams("Regression")
  boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
  boostingStrategy.treeStrategy.maxDepth = 5
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

  val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val labelsAndPredictions = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
  println("Test Mean Squared Error = " + testMSE)
  println("Learned regression GBT model:\n" + model.toDebugString)

  val out = new PrintWriter("gbt-regression-result.txt")
  out.println("test length = " + testData.collect().length)
  testData.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "gbt-regression-model")
  val sameModel = DecisionTreeModel.load(sc, "gbt-regression-model")

}
