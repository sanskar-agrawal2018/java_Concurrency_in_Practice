import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.{Callable, Executors, LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer

object Master {

  private val DriverCores  = 2
  private val NumExecutors = 2
  private val ExecutorMem  = "1g"
  private val Port         = 5555

  def main(args: Array[String]): Unit = {
    val expectedSum = (1 to 1_000_000).map(_.toLong).sum
    val array       = (1 to 1_000_000).toArray
    println(s"[Master] cores=$DriverCores executors=$NumExecutors expectedSum=$expectedSum")

    val server = new ServerSocket(Port)
    println(s"[Master] listening on $Port")

    val executorProcs = launchExecutors()

    // ---- single listener thread that accepts executor connections ----
    val incoming = new LinkedBlockingQueue[Socket]()
    val listener = new Thread(new Runnable {
      override def run(): Unit = {
        var connected = 0
        while (connected < NumExecutors) {
          val s = server.accept()
          println(s"[Master][listener] executor connected from ${s.getRemoteSocketAddress}")
          incoming.put(s)
          connected += 1
        }
      }
    }, "executor-listener")
    listener.start()

    val sockets = ArrayBuffer.empty[Socket]
    while (sockets.size < NumExecutors) sockets += incoming.take()
    listener.join()

    // ---- driver thread pool (2 cores) dispatches work and gathers results ----
    val driverPool = Executors.newFixedThreadPool(DriverCores)
    val chunkSize  = (array.length + NumExecutors - 1) / NumExecutors
    val chunks     = array.grouped(chunkSize).toArray

    val futures = chunks.zip(sockets).zipWithIndex.map { case ((chunk, sock), idx) =>
      driverPool.submit(new Callable[Long] {
        override def call(): Long = {
          val out = new ObjectOutputStream(sock.getOutputStream)
          out.flush()
          val in = new ObjectInputStream(sock.getInputStream)
          out.writeObject(chunk)
          out.flush()
          val partial = in.readLong()
          println(s"[Master] partial from executor-$idx = $partial")
          partial
        }
      })
    }

    val total = futures.map(_.get()).sum
    println(s"[Master] TOTAL = $total  (match=${total == expectedSum})")

    driverPool.shutdown()
    driverPool.awaitTermination(5, TimeUnit.SECONDS)
    sockets.foreach(_.close())
    server.close()
    executorProcs.foreach(_.waitFor(10, TimeUnit.SECONDS))
  }

  private def launchExecutors(): Seq[Process] = {
    val javaBin = System.getProperty("java.home") + "/bin/java"
    val cp      = System.getProperty("java.class.path")
    (0 until NumExecutors).map { id =>
      val cmd = new java.util.ArrayList[String]()
      cmd.add(javaBin)
      cmd.add(s"-Xmx$ExecutorMem")
      cmd.add(s"-Xms$ExecutorMem")
      cmd.add("-cp"); cmd.add(cp)
      cmd.add("Executor")
      cmd.add("localhost"); cmd.add(Port.toString); cmd.add(id.toString)
      val pb = new ProcessBuilder(cmd).inheritIO()
      println(s"[Master] launching executor-$id with -Xmx$ExecutorMem")
      pb.start()
    }
  }
}
