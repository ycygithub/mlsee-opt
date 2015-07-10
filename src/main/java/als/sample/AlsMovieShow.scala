package als.sample

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object AlsMovieShow extends App{

  val conf = new SparkConf().setAppName("Als Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data = sc.textFile("data/mllib/als/sample_movielens_ratings.txt")
  val ratings = data.map(_.split("::") match { case Array(user, item, rate, ts) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })

  // Build the recommendation model using ALS
  val rank = 10
  val numIterations = 3
  val model = ALS.train(ratings, rank, numIterations, 0.01)

  // Evaluate the model on rating data
  val usersProducts = ratings.map { case Rating(user, product, rate) =>
    (user, product)
  }

  val predictions =
    model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictions)

  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
  }.mean()

  println("Mean Squared Error = " + MSE)
  println("model.predict(1,2)-->"+model.predict(1,2))

  // Save and load model
  model.save(sc, "model")
  val sameModel = MatrixFactorizationModel.load(sc, "model")

}
