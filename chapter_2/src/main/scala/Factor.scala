object  Factor {
  println("Object created ")

}


object FactorTest {
  println("FactorTest object created")
  def main(args: Array[String]): Unit = {
    println("FactorTest main method started")
    println(s"Accessing Factor object: ${Factor.toString}")
    println("FactorTest main method finished")
  }
}