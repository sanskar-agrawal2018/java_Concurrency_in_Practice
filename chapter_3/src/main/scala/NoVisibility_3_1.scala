
object NoVisibility_3_1 {

  var flag = false
  var value = -1

  def changeFlag(): Unit = {
    value = 42
    flag = true
    println("Flag changed to true and value set to 42")
  }
}



object  NoVisibilityRun {

  def main(args: Array[String]): Unit = {
    val t1 = new Thread(() => {
      println("Thread 1: Changing flag to true")

      NoVisibility_3_1.changeFlag()
    })


    val t2 = new Thread(() => {
      var loop_count = 0
      while (!NoVisibility_3_1.flag) {
        loop_count += 1
        println(s"Thread 2: Flag is false, loop count: $loop_count")
        // Busy-wait until flag becomes true
      }
      println(s"Loop count: $loop_count")
      println(s"Thread 2: Detected flag is true, value is ${NoVisibility_3_1.value}")
    })

    val thread= Seq(t1,t2)
    thread.foreach(_.start())
    thread.foreach(_.join())




  }
}
