package search

import com.typesafe.config.ConfigException
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import search.classification.{ClassificationRegistrator, Classification}
import search.constant.Constant

object Main extends Logging{

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.out.println("Usage: Error Tag!")
      System.exit(-1)
    }

    val tag = args(0)
    //val conf = ConfigUtil.getDefaultConfig
    val currentTime = System.currentTimeMillis

    if (args.length > 1 && args(1).toBoolean) {
      System.setProperty("spark.serializer", classOf[KryoSerializer].getName)
      System.setProperty("spark.kryo.registrator", classOf[ClassificationRegistrator].getName)
      System.setProperty("spark.kryoserializer.buffer.mb", "8")
    }

    val sparkConf = new SparkConf
    sparkConf.setAppName("Classification")
    var sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    try {

      //val checkpoint = conf.getString("deal_cf.als.checkpoint.dir")
      val checkpoint = Constant.CHECKPOINT_DIR

      if (checkpoint.length > 1) {
        sc.setCheckpointDir(checkpoint)
        logInfo(s"setCheckpointDir: ${checkpoint}")
      }
    } catch {
      case e: ConfigException.Missing =>
        logWarning(e.getMessage, e)
    }

    try {
      Classification.gcCleaner(tag)
      Classification.etl(sqlContext, tag, currentTime)
      Classification.train(sqlContext, tag, currentTime)
      Classification.dealRecommend(sqlContext, tag, currentTime)

      sc.stop()
      sc = null
      logInfo("Classification SUCCESSFUL")
      sys.exit(0)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        logInfo("Classification ERROR")
        sys.exit(-1)
    } finally {
      if (sc != null) sc.stop()
    }

  }

}
