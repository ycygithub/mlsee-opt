
object TMap extends App{

  def printSortValues[T:Ordering, U:Ordering](m : Map[T,U]) {
    m.toArray.sortBy(x => (x._2,x._1)).foreach(println)
  }

  def printSortValues2[T:Ordering, U:Ordering](m : Map[T,U]) {
    println(m.size)
    m.toList.sortBy(_._2).reverse.reverse.zipWithIndex.foreach(f => {
      println(f._1+","+f._2)
    })
  }

  var maps = Map[Int, Double]()

  maps += (1 -> 4.1)
  maps += (3 -> 3.1)
  maps += (2 -> 3.9)
  maps.+=(4 -> 4.2)

  println(maps.get(3))
  maps.getOrElse(3,1.2)
  println(maps.get(3))

  var maps2 = scala.collection.mutable.Map[Int,Double]()

  maps2 += (1 -> 4.1)
  maps2 += (3 -> 3.1)
  maps2 += (2 -> 3.9)
  maps2.+=(4 -> 4.2)

  println(maps2.get(3))
  maps2.getOrElseUpdate(3,1.2)
  println(maps2.get(3))

  for(k <- maps2.keySet){
    println(k)
  }

  printSortValues(maps)
  println()
  printSortValues2(maps)

}
