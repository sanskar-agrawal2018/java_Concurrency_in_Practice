import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}

/**
 * ExecutorShutdownDemo
 *
 * Demonstrates why it's important to shut down ExecutorServices and how to do it.
 * Run one demo at a time (so you can observe JVM exit behavior):
 *
 *   sbt "chapter6/runMain ExecutorShutdownDemo no-shutdown"
 *   sbt "chapter6/runMain ExecutorShutdownDemo shutdown"
 *   sbt "chapter6/runMain ExecutorShutdownDemo shutdown-now"
 *   sbt "chapter6/runMain ExecutorShutdownDemo daemon"
 *
 * Notes:
 *  - Default thread pools create non-daemon threads; the JVM will wait for them
 *    to finish before exiting. Failing to call shutdown() can keep your process
 *    alive unintentionally.
 *  - Making worker threads DAEMON allows the JVM to exit even if tasks are
 *    still running; use with care because work may be lost.
 */
object ExecutorShutdownDemo {
  def main(args: Array[String]): Unit = {
    val mode = if (args.length > 0) args(0).toLowerCase else "usage"
    mode match {
      case "no-shutdown"   => demoNoShutdown()
      case "shutdown"      => demoShutdownGraceful()
      case "shutdown-now"  => demoShutdownNow()
      case "daemon"        => demoDaemonThreads()
      case _                => printUsage()
    }
  }

  private def printUsage(): Unit = {
    println("Usage: runMain ExecutorShutdownDemo <mode>")
    println("Modes:")
    println("  no-shutdown    - submit tasks and DO NOT call shutdown(); JVM will wait for non-daemon threads")
    println("  shutdown       - submit tasks and call shutdown() + awaitTermination() to stop the executor gracefully")
    println("  shutdown-now   - submit tasks and call shutdownNow(), demonstrating interruption")
    println("  daemon         - use an executor whose threads are DAEMON; JVM will exit even if tasks are running")
  }

  // Helper task that sleeps and prints progress
  private def longRunningTask(id: Int, sleepMs: Long = 3000): Runnable = new Runnable {
    override def run(): Unit = {
      val tName = Thread.currentThread().getName
      println(s"[$tName] Task-$id START (sleeping $sleepMs ms)")
      try {
        Thread.sleep(sleepMs)
        println(s"[$tName] Task-$id FINISH")
      } catch {
        case _: InterruptedException =>
          println(s"[$tName] Task-$id INTERRUPTED")
          Thread.currentThread().interrupt()
      }
    }
  }

  private def demoNoShutdown(): Unit = {
    println("=== demoNoShutdown ===")
    val exec = Executors.newFixedThreadPool(2)

    exec.submit(longRunningTask(1, 3000))
    exec.submit(longRunningTask(2, 4000))

    println("[main] Submitted tasks but NOT calling shutdown(). main() will return now.")
    println("[main] Because the pool uses NON-DAEMON threads, the JVM will wait for them to finish before exiting.")
    // Return; observe that program does not terminate until worker threads finish
  }

  private def demoShutdownGraceful(): Unit = {
    println("=== demoShutdownGraceful ===")
    val exec = Executors.newFixedThreadPool(2)

    exec.submit(longRunningTask(1, 2000))
    exec.submit(longRunningTask(2, 2000))

    println("[main] Calling shutdown() to stop accepting new tasks and finish existing ones.")
    exec.shutdown() // Disable new tasks, keep running existing

    try {
      val finished = exec.awaitTermination(5, TimeUnit.SECONDS)
      println(s"[main] awaitTermination returned: $finished")
      if (!finished) {
        println("[main] Not finished within timeout; calling shutdownNow() as fallback")
        val pending = exec.shutdownNow()
        println(s"[main] shutdownNow returned ${pending.size()} pending tasks")
      }
    } catch {
      case _: InterruptedException =>
        println("[main] awaitTermination interrupted; calling shutdownNow()")
        exec.shutdownNow()
        Thread.currentThread().interrupt()
    }

    println("[main] demoShutdownGraceful complete; JVM can exit once non-daemon workers finish (or now if none remain)")
  }

  private def demoShutdownNow(): Unit = {
    println("=== demoShutdownNow ===")
    val exec = Executors.newFixedThreadPool(2)

    exec.submit(longRunningTask(1, 10000)) // long running
    exec.submit(longRunningTask(2, 10000))

    // Give tasks a moment to start
    Thread.sleep(200)
    println("[main] Calling shutdownNow() which attempts to cancel running tasks and returns pending ones.")
    val pending = exec.shutdownNow()
    println(s"[main] shutdownNow returned ${pending.size()} pending tasks")

    try {
      val finished = exec.awaitTermination(2, TimeUnit.SECONDS)
      println(s"[main] awaitTermination returned: $finished")
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }

    println("[main] demoShutdownNow complete")
  }

  private def daemonThreadFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val counter = new java.util.concurrent.atomic.AtomicInteger(1)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"${prefix}-${counter.getAndIncrement()}")
      t.setDaemon(true)
      t
    }
  }

  private def demoDaemonThreads(): Unit = {
    println("=== demoDaemonThreads ===")
    val exec = Executors.newFixedThreadPool(2, daemonThreadFactory("daemon-worker"))

    exec.submit(longRunningTask(1, 5000))
    exec.submit(longRunningTask(2, 5000))

    println("[main] Submitted tasks to executor using DAEMON threads and NOT calling shutdown().")
    println("[main] Because threads are daemon, the JVM may exit immediately after main returns, potentially killing background tasks.")
    println("[main] main() will return now. You should observe that tasks MAY NOT complete.")
    // Return; process will typically exit immediately because only daemon threads remain
  }
}

