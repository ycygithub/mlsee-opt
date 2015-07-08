package base

object CountLines extends App{

  val src = scala.io.Source.fromFile("data/mllib/als/test.data")
  val count = src.getLines().map(x => 1).sum
  println(count)

}
