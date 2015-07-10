package als.app

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object AlsMovie extends App{

  val conf = new SparkConf().setAppName("Als Application").setMaster("local").set("spark.executor.memory", "8g")
  val sc = new SparkContext(conf)

  //加载数据
  val data = sc.textFile("als/ml-100k/ratings.dat")

  //data中每条数据经过map的split后会是一个数组，模式匹配后，会new一个Rating对象
  val ratings = data.map(_.split("::") match { case Array(user, item, rate, ts) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })

  //创建模型
  val rank = 10
  val numIterations = 3
  val model = ALS.train(ratings, rank, numIterations, 0.01)

  //评估模型
  val usersProducts = ratings.map { case Rating(user, product, rate) =>
    (user, product)
  }

  //预测
  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }

  //混合
  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictions)

  //计算均方根误差
  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
  }.mean()

  println("Mean Squared Error = " + MSE)
  println("model.predict(1,2)-->"+model.predict(1,2))

  //保存和加载模型
  model.save(sc, "movie-model")
  val sameModel = MatrixFactorizationModel.load(sc, "movie-model")

}
