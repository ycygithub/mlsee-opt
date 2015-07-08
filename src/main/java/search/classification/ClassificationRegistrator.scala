package search.classification

import breeze.linalg.{DenseVector => BDV}
import com.esotericsoftware.kryo.Kryo
import com.typesafe.config.Config
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

class ClassificationRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Rating])
    kryo.register(classOf[Config])
    kryo.register(classOf[BDV[Double]])
    kryo.register(classOf[mutable.BitSet])
  }
}
