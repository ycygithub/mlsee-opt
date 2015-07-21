package linear.sample

import java.io.PrintWriter

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionModel, LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegressionShow extends App{

  val conf = new SparkConf().setAppName("Spark Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)


  // Load and parse the data
  val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
  val parsedData = data.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
  }.cache()

  // Building the model
  val numIterations = 100
  val model = LinearRegressionWithSGD.train(parsedData, numIterations)

  // Evaluate model on training examples and compute training error
  val valuesAndPreds = parsedData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  val out = new PrintWriter("linear-result.txt")
  out.println("parsedData length = " + parsedData.collect().length)
  parsedData.foreach(e => out.println("model.predict("+e.label+")-->"+model.predict(e.features)))
  out.close()

  val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
  println("training Mean Squared Error = " + MSE)

  // Save and load model
  model.save(sc, "linear-model")
  val sameModel = LinearRegressionModel.load(sc, "linear-model")

}
