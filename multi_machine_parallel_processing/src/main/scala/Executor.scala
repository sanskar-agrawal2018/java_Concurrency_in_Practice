import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.Socket
import java.util.concurrent.{Callable, Executors, TimeUnit}

object Executor {

  private val Cores = 2

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val id   = args(2)

    val maxMb = Runtime.getRuntime.maxMemory() / 1024 / 1024
    println(s"[Executor-$id] up. cores=$Cores heapMax=${maxMb}MB pid=${ProcessHandle.current().pid()}")

    val sock = new Socket(host, port)
    val out  = new ObjectOutputStream(sock.getOutputStream); out.flush()
    val in   = new ObjectInputStream(sock.getInputStream)

    val chunk = in.readObject().asInstanceOf[Array[Int]]
    println(s"[Executor-$id] received ${chunk.length} elements")

    val pool        = Executors.newFixedThreadPool(Cores)
    val subSize     = (chunk.length + Cores - 1) / Cores
    val subChunks   = chunk.grouped(subSize).toArray

    val futures = subChunks.zipWithIndex.map { case (sc, tIdx) =>
      pool.submit(new Callable[Long] {
        override def call(): Long = {
          val tname = Thread.currentThread().getName
          var s     = 0L
          var i     = 0
          while (i < sc.length) { s += sc(i); i += 1 }
          println(s"[Executor-$id][thread-$tIdx/$tname] partial=$s n=${sc.length}")
          s
        }
      })
    }

    val sum = futures.map(_.get()).sum
    println(s"[Executor-$id] chunkSum=$sum -> sending to master")
    out.writeLong(sum); out.flush()

    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.SECONDS)
    sock.close()
  }
}
