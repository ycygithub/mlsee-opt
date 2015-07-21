package tree.sample

import java.io.PrintWriter

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeRegressionShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)


  // Load and parse the data file.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a DecisionTree model.
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = "variance"
  val maxDepth = 5
  val maxBins = 32

  val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
    maxDepth, maxBins)

  // Evaluate model on test instances and compute test error
  val labelsAndPredictions = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
  println("Test Mean Squared Error = " + testMSE)
  println("Learned regression tree model:\n" + model.toDebugString)

  val out = new PrintWriter("decision-tree-regression-result.txt")
  out.println("test length = " + testData.collect().length)
  testData.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "decision-tree-regression-model")
  val sameModel = DecisionTreeModel.load(sc, "decision-tree-regression-model")

}
