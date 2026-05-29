class Hello(name: String) {

  println(s"Creating a new Hello instance with name: $name")
  def print(): Unit = {
    println(s"Hello, $name!")
  }
}


object Hello {
  var x=0
  def apply(name: String): Hello = {
    val p= new Hello(name)
    val y=x
//    Thread.sleep(5000)
    x=y+1
    p

  }
}

object print_Hello {
  def main(args: Array[String]): Unit = {
    println("Starting to create Hello instances...")
    val threads = (1 to 5).map { i =>
      new Thread(new Runnable {
        override def run(): Unit = {
          println(s"Creating Hello instance for User$i")
          val h = Hello(s"User$i")
          h.print()
        }
      })
    }

    threads.foreach(_.start())
    //Wait for all threads to finish before printing the final value of x
    threads.foreach(_.join())
    println(Hello.x)
  }
}