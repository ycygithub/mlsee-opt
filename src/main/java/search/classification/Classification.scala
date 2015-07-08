package search.classification

import java.lang.ref.WeakReference
import java.lang.{Long => JLong}
import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone, Timer, TimerTask}

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import search.constant.Constant
import search.item.{Estimator, IDRescorer, LongPrimitiveArrayIterator, TopItems}

import scala.collection.JavaConversions._
import scala.io.Source


object Classification extends Logging{

  def filterHeader(dealId: String, createTime: String): Boolean = {
    dealId.length < 10 && dealId.matches( """\d+""") && createTime.matches( """\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}""")
  }

  def clickPreferences(sqlContext: SQLContext, dealOutPath: String, periodTime: Long,
                       currentTime: Long): RDD[(String, (Int, Float))] = {
    val sc = sqlContext.sparkContext
    val filterUser = Seq("02:00:00:00:00:00", "NULL", "null", "111111111111111", "000000000000000", "41951164",
      "0", "1", "Unknown", "004999010640000", "18786371401391689537", "6380847431404378962",
      "3150874581404005312", "6334393121395216829", "2032571811403082538", "004999010640000", "00000000",
      "11111111111111", "111111111111119", "", "-1", "-2")
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+08:00"))
    val df = sqlContext.parquetFile(dealOutPath).select("uid", "click_deal_id", "createtime", "keyword")
    df.filter("uid IS NOT NULL AND uid !='' ").rdd.map {
      case Row(uid: String, dealId: String, createAt: String, keyWord: String) =>
        (uid, dealId, createAt, keyWord)
    }.filter(t => filterHeader(t._2, t._3)).map { case (uid, dealId, createAt, keyWord) =>
      val time = simpleDateFormat.parse(createAt).getTime
      (uid, dealId.toInt, time, keyWord.toLowerCase.trim)
    }.filter { case (uid, dealId, createAt, keyWord) =>
      dealId > 0 && uid != null && uid.size < 512 &&
        !filterUser.contains(uid.trim) && ((currentTime - createAt) < periodTime)
    }.map { case (uid, dealId, createAt, keyWord) =>
      val preference = 1F / (1F + ((currentTime - createAt) /
        (1000L * 60 * 60 * 24)).abs.toFloat)
      (keyWord, (dealId, preference))
    }
  }


  def deal2SubTagId(sc: SparkContext, currentTime: Long): Map[Int, Int] = {
    val periodTime = Constant.ALS_MAXPERIODTIME
    val dealInfoPath = Constant.DEAL_INFO
    val dealInfos = sc.textFile(dealInfoPath).mapPartitions {
      itr =>
        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT)
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+08:00"))
        itr.map {
          line =>
            val array = line.split("\\|", -1)
            (array(0), array(18), array(2))
        }.filter(t => {
          filterHeader(t._1, t._3) && t._2.matches( """\d+""")
        }).filter { t =>
          val time = simpleDateFormat.parse(t._3).getTime
          (currentTime - time) < periodTime
        }.map(t => (t._1.toInt, t._2.toInt))
    }.collect.toMap
    dealInfos
  }


  def gcCleaner(tag: String) {
    val timer = new Timer(tag + " cleanup timer", true)
    val task = new TimerTask {
      override def run() {
        try {
          runGC
          logInfo("Ran metadata cleaner for " + tag)
        } catch {
          case e: Exception => logError("Error running cleanup task for " + tag, e)
        }
      }

      /** Run GC and make sure it actually has run */
      def runGC() {
        val weakRef = new WeakReference(new Object())
        val startTime = System.currentTimeMillis
        // Make a best effort to run the garbage collection. It *usually* runs GC.
        // Wait until a weak reference object has been GCed
        System.gc()
        System.runFinalization()
        while (weakRef.get != null) {
          System.gc()
          System.runFinalization()
          Thread.sleep(200)
          if (System.currentTimeMillis - startTime > 10000) {
            throw new Exception("automatically cleanup error")
          }
        }
      }
    }

    timer.schedule(task, Constant.GCCLEANER_DELAY * 1000, Constant.GCCLEANER_DELAY * 1000)
  }


  def etl(sqlContext: SQLContext, tag: String, currentTime: Long) {
    val sc = sqlContext.sparkContext

//    val dealOutPath = conf.getString("search_Term_classification.zhe_click_sup_d")
//    val uidIndexPath = s"${conf.getString("search_Term_classification.user_index")}/$tag"
//    val ratingPath = s"${conf.getString("search_Term_classification.rating")}/${tag}"
//    val periodTime = conf.getLong("search_Term_classification.als.maxPeriodTime")

//    if (existSuccessFile(sc, uidIndexPath) && existSuccessFile(sc, ratingPath))
//      return

    val dealOutPath = Constant.ZHE_CLICK_SUP_D
    val uidIndexPath = s"${Constant.USER_INDEX}/$tag"
    val ratingPath = s"${Constant.RATING}/${tag}"
    val periodTime = Constant.ALS_MAXPERIODTIME

//    val uidFileSystem = new Path(uidIndexPath).getFileSystem(sc.hadoopConfiguration)
//    val ratingFileSystem = new Path(ratingPath).getFileSystem(sc.hadoopConfiguration)
//    uidFileSystem.delete(new Path(uidIndexPath), true)
//    ratingFileSystem.delete(new Path(ratingPath), true)

    val uidFileSystem = Source.fromFile(uidIndexPath)
    val ratingFileSystem = Source.fromFile(ratingPath)

    val allBehaviors = clickPreferences(sqlContext, dealOutPath, periodTime, currentTime)
    allBehaviors.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val dealId2TagId = deal2SubTagId(sc, currentTime)
    val mDealIds = sc.broadcast(dealId2TagId)
    val maxSubTagId = dealId2TagId.values.max + 1
    val peferences = allBehaviors.mapPartitions { iter =>
      val map = mDealIds.value
      iter.filter(t => map.contains(t._2._1)).map {
        case (keyWord, (dealId, preference)) =>
          val subTagId = map(dealId)
          val sv = BSV.zeros[Double](maxSubTagId)
          sv(subTagId) = preference
          (keyWord, sv)
      }
    }.reduceByKey(_ :+ _).map { case (keyWord, sv) =>
      // sv :/= brzNorm(sv, 2)
      (keyWord, sv)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    var uidIndex = peferences.map(_._1).distinct().zipWithIndex().map(t => (t._1, t._2.toInt))
    uidIndex.map {
      case (uid, index) =>
        s"$index,$uid"
    }.saveAsTextFile(uidIndexPath)

    uidIndex = sc.textFile(uidIndexPath).map(_.split(",")).map {
      ts =>
        (ts.tail.mkString(","), ts.head.toInt)
    }
    peferences.join(uidIndex).flatMap {
      case (uid, (preference, index)) =>
        preference.activeIterator.map { case (subTagId, value) =>
          s"$index,$subTagId,$value"
        }
    }.saveAsTextFile(ratingPath)
    peferences.unpersist()
    allBehaviors.unpersist()
  }


  def train(sqlContext: SQLContext, tag: String, currentTime: Long): Unit = {

    val sc = sqlContext.sparkContext

//    val uidIndexPath = s"${conf.getString("search_Term_classification.user_index")}/$tag"
//    val ratingPath = s"${conf.getString("search_Term_classification.rating")}/${tag}"
//    val periodTime = conf.getLong("search_Term_classification.als.maxPeriodTime")
//    val modelPath = s"${conf.getString("search_Term_classification.model")}/$tag"
//    val rank = conf.getInt("search_Term_classification.als.rank")
//    val iterations = conf.getInt("search_Term_classification.als.iterations")

    val uidIndexPath = s"${Constant.USER_INDEX}/$tag"
    val ratingPath = s"${Constant.RATING}/${tag}"
    val periodTime = Constant.ALS_MAXPERIODTIME
    val modelPath = s"${Constant.MODEL}/$tag"
    val rank = Constant.ALS_RANK
    val iterations = Constant.ALS_ITERATIONS

//    if (existSuccessFile(sc, modelPath + "/keyWordFeatures") &&
//      existSuccessFile(sc, modelPath + "/subTagFeatures"))
//      return

//    val fs = new Path(modelPath).getFileSystem(sc.hadoopConfiguration)
//    fs.delete(new Path(modelPath, "keyWordFeatures"), true)
//    fs.delete(new Path(modelPath, "subTagFeatures"), true)

    val fs = Source.fromFile(modelPath)

    val uidIndex = sc.textFile(uidIndexPath).map(_.split(",")).map {
      ts =>
        (ts.head.toInt, ts.tail.mkString(","))
    }

    val ratings = sc.textFile(ratingPath).map {
      line =>
        val Array(keyWordId, subTagId, preference) = line.split(",")
        new Rating(keyWordId.toInt, subTagId.toInt, preference.toDouble)
    }

    ratings.setName("ratings").persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"train ratings size: ${ratings.count()}")

    val model = ALS.train(ratings, rank, iterations, 0.01)

    model.userFeatures.join(uidIndex).map {
      case (index, (features, uid)) =>
        s"$uid|${features.mkString(",")}"
    }.repartition(60).saveAsTextFile(modelPath + "/keyWordFeatures")
    logInfo("user size: " + model.userFeatures.count())

    model.productFeatures.map {
      case (index, features) =>
        s"$index|${features.mkString(",")}"
    }.repartition(20).saveAsTextFile(modelPath + "/subTagFeatures")
    logInfo("product size: " + model.productFeatures.count())

    ratings.unpersist()
  }


  def dealRecommend(sqlContext: SQLContext, tag: String, currentTime: Long): Unit = {
    val sc = sqlContext.sparkContext
    val recommendPath = s"${Constant.UID_RECOMMEND}/$tag"

//    if (existSuccessFile(sc, recommendPath))
//      return

//    val recommendFileSystem = new Path(recommendPath).getFileSystem(sc.hadoopConfiguration)
    val recommendFileSystem = Source.fromFile(recommendPath)

    val howMany = Constant.ALS_SIZE
    val modelPath = s"${Constant.MODEL}/$tag"
    val dealId2TagId = deal2SubTagId(sc, currentTime)
    val userFeatures = sc.textFile(modelPath + "/keyWordFeatures").map {
      line =>
        val ls = line.split('|')
        (ls.head, new BDV[Double](ls.last.split(',').map(_.toDouble)))
    }

    val subTagFeatures = sc.textFile(modelPath + "/subTagFeatures").map {
      line =>
        val ls = line.split('|')
        (ls.head.toLong, ls.last)
    }.mapPartitions {
      itr =>
        itr.map {
          case (dealId, doubles) =>
            (dealId, new BDV[Double](doubles.split(',').map(_.toDouble)))
        }
    }.collect().toMap
    val broadcast = sc.broadcast(subTagFeatures)

    val userRec = userFeatures.repartition(sc.defaultParallelism).mapPartitions {
      itr =>
        val rescorer = new IDRescorer {
          override def rescore(id: Long, originalScore: Double): Double = originalScore

          override def isFiltered(id: Long): Boolean = false
        }
        val m = broadcast.value
        val dealIds = new LongPrimitiveArrayIterator(m.keysIterator.toArray)
        itr.map {
          case (uid, userFeatures) =>
            dealIds.reset()
            val estimator = new Estimator[JLong] {
              override def estimate(dealID: JLong): Double = {
                val sim = m(dealID).dot(userFeatures)
                sim
              }
            }
            val topItems = TopItems.getTopItems(howMany, dealIds, rescorer, estimator)
            (uid, topItems)
        }.filter(_._2.nonEmpty)
    }.persist(StorageLevel.MEMORY_AND_DISK)
    userRec.map { case (uid, topItems) =>
      s"$uid|${topItems.map(t => t.getItemID + ":" + t.getValue).mkString(",")}"
    }.saveAsTextFile(recommendPath)
    userRec.unpersist()
  }


}
