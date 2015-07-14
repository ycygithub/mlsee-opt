import scala.collection.mutable.ListBuffer


object TListBuffer extends App{

  var l = new ListBuffer[Int]()

  for(i <- 0 to 9){
    l += i
  }

  for(i <- 0 to 10){
    if(!l.contains(i)) l += i
  }

  println(l.size)
  l.foreach(e => println(e))

  for(i <- 0 until l.size){
    println(l.apply(i))
  }

}
