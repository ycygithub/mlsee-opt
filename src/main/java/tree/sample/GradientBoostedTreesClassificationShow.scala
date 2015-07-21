package tree.sample

import java.io.PrintWriter

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object GradientBoostedTreesClassificationShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data file.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a GradientBoostedTrees model.
  //  The defaultParams for Classification use LogLoss by default.
  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
  boostingStrategy.treeStrategy.numClasses = 2
  boostingStrategy.treeStrategy.maxDepth = 5
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

  val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
  println("Test Error = " + testErr)
  println("Learned classification GBT model:\n" + model.toDebugString)

  val out = new PrintWriter("gbt-classification-result.txt")
  out.println("test length = " + testData.collect().length)
  testData.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  // Save and load model
  model.save(sc, "gbt-classification-model")
  val sameModel = DecisionTreeModel.load(sc, "gbt-classification-model")

}
