package als.app

import java.io.PrintWriter

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object AlsMovie extends App{

  val conf = new SparkConf().setAppName("Als Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  val data = sc.textFile("data/mllib/als/sample_movielens_ratings.txt")

  val ratings = data.map(_.split("::") match { case Array(user, item, rate, ts) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })

  val rank = 10
  val numIterations = 3
  val model = ALS.train(ratings, rank, numIterations, 0.01)

  val usersProducts = ratings.map { case Rating(user, product, rate) =>
    (user, product)
  }

  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
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

  var ul = new ListBuffer[Int]()
  var pl = new ListBuffer[Int]()

  val u = ratings.map { case Rating(user, product, rate) => (user) }
  val p = ratings.map { case Rating(user, product, rate) => (product) }

  u.foreach(e => if(!ul.contains(e)) ul.+=(e))
  p.foreach(e => if(!pl.contains(e)) pl.+=(e))

  println("ul size " + ul.size)
  println("pl size " + pl.size)

  val out = new PrintWriter("result.txt")

  ul.foreach(u =>{
    out.print(u+"|")
    var maps = Map[Int, Double]()
    pl.foreach(p =>{
      maps.+=(p -> model.predict(u,p))
    })
    maps.toList.sortBy(_._2).reverse.zipWithIndex.foreach(e => {
      if(e._2 != maps.size -1){
        out.print(e._1._1+":"+e._1._2+",")
      }else{
        out.print(e._1._1+":"+e._1._2)
      }
    })
    out.println()
  })

  out.close()
  model.save(sc, "movie-model")
  val sameModel = MatrixFactorizationModel.load(sc, "movie-model")

}
