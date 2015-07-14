import scala.collection.mutable.ListBuffer


object TListBuffer2 extends App{

   var l = new ListBuffer[Int]()

   for(i <- 0 to 9){
     l.+=(i)
   }

   for(i <- 0 to 10){
     if(!l.contains(i)) l.+=(i)
   }

   println(l.size)
   l.foreach(e => println(e))

 }
