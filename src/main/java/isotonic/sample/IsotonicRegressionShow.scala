package isotonic.sample

import java.io.PrintWriter

import org.apache.spark.mllib.regression._
import org.apache.spark.{SparkConf, SparkContext}

object IsotonicRegressionShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)


  val data = sc.textFile("data/mllib/sample_isotonic_regression_data.txt")

  // Create label, feature, weight tuples from input data with weight set to default value 1.0.
  val parsedData = data.map { line =>
    val parts = line.split(',').map(_.toDouble)
    (parts(0), parts(1), 1.0)
  }

  // Split data into training (60%) and test (40%) sets.
  val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0)
  val test = splits(1)

  // Create isotonic regression model from training data.
  // Isotonic parameter defaults to true so it is only shown for demonstration
  val model = new IsotonicRegression().setIsotonic(true).run(training)

  // Create tuples of predicted and real labels.
  val predictionAndLabel = test.map { point =>
    val predictedLabel = model.predict(point._2)
    (predictedLabel, point._1)
  }

  // Calculate mean squared error between predicted and real labels.
  val meanSquaredError = predictionAndLabel.map{case(p, l) => math.pow((p - l), 2)}.mean()
  println("Mean Squared Error = " + meanSquaredError)

  val out = new PrintWriter("isotonic-result.txt")
  out.println("parsedData length = " + test.collect().length)
  test.foreach(e => out.println("model.predict("+e._2+")-->"+model.predict(e._2)))
  out.close()

  // Save and load model
  model.save(sc, "isotonic-model")
  val sameModel = IsotonicRegressionModel.load(sc, "isotonic-model")

}
