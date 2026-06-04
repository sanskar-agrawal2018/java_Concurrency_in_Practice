/**
 * Chapter 7 -- Demonstrate Future.cancel() for tasks in different states
 *
 * This demo shows three situations:
 *  1) cancel a queued (not yet running) task -> should be removed from queue and cancelled
 *  2) cancel a running task with cancel(true) -> underlying thread is interrupted
 *  3) cancel a task after it already completed -> cancel() returns false and has no effect
 *
 * Run:
 *   sbt "chapter7/runMain CancelStatesDemo"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

object CancelStatesDemo {

  def main(args: Array[String]): Unit = {
    println("=== Cancel queued task (waiting to be assigned) ===")
    demoCancelQueued()
    sep()

    println("=== Cancel running task (interrupt underlying thread) ===")
    demoCancelRunning()
    sep()

    println("=== Cancel after completion (no-op) ===")
    demoCancelAfterCompletion()
  }

  // Cancel a task while it's still queued (not yet assigned to a worker thread).
  private def demoCancelQueued(): Unit = {
    val pool = Executors.newFixedThreadPool(1, namedFactory("queued-demo"))
    try {
      // Long task occupies the only thread
      val longTask = new Callable[String] {
        override def call(): String = {
          println(s"[${Thread.currentThread.getName}] longTask started (occupies thread)")
          Thread.sleep(1500)
          println(s"[${Thread.currentThread.getName}] longTask finishing")
          "long-done"
        }
      }

      val longFuture = pool.submit(longTask)

      // Submit a second task which will be queued
      val queuedTask = new Callable[String] {
        override def call(): String = {
          println(s"[${Thread.currentThread.getName}] queuedTask started (should NOT run if cancelled)")
          "queued-done"
        }
      }

      val queuedFuture = pool.submit(queuedTask)

      // Cancel immediately while it's likely still in the queue
      val cancelResult = queuedFuture.cancel(false) // don't interrupt (not running yet)
      println(s"[main] queuedFuture.cancel(false) returned $cancelResult")
      println(s"[main] queuedFuture.isCancelled = ${queuedFuture.isCancelled}, isDone = ${queuedFuture.isDone}")

      try queuedFuture.get() catch {
        case _: CancellationException => println("[main] queuedFuture.get() threw CancellationException as expected")
      }

      // Let the long task finish so pool can be shutdown cleanly
      println("[main] waiting for longTask to finish...")
      println(s"[main] longFuture.get() = ${longFuture.get()}")
    } finally {
      shutdownPool(pool)
    }
  }

  // Cancel a task while it is running. Use cancel(true) to interrupt the executing thread.
  private def demoCancelRunning(): Unit = {
    val pool = Executors.newFixedThreadPool(1, namedFactory("running-demo"))
    try {
      val processed = new AtomicInteger(0)

      val interruptibleLong = new Callable[String] {
        override def call(): String = {
          println(s"[${Thread.currentThread.getName}] interruptibleLong started")
          try {
            while (processed.get() < 10) {
              Thread.sleep(200) // sleeping cooperates with interruption
              val n = processed.incrementAndGet()
              println(s"[${Thread.currentThread.getName}] progress $n")
            }
          } catch {
            case _: InterruptedException =>
              println(s"[${Thread.currentThread.getName}] observed InterruptedException; exiting early")
              Thread.currentThread().interrupt()
          }
          s"processed=${processed.get()}"
        }
      }

      val f = pool.submit(interruptibleLong)

      // Let it run a little so it becomes RUNNING
      Thread.sleep(450)
      println(s"[main] issuing f.cancel(true) while task is likely running")
      val cancelled = f.cancel(true)
      println(s"[main] f.cancel(true) returned $cancelled; isCancelled=${f.isCancelled}, isDone=${f.isDone}")

      try f.get(1, TimeUnit.SECONDS) catch {
        case _: CancellationException => println("[main] f.get() threw CancellationException as expected")
        case _: TimeoutException => println("[main] f.get() timed out; task may not have stopped")
      }

      println(s"[main] processed value after cancel attempt = ${processed.get()}")
    } finally {
      shutdownPool(pool)
    }
  }

  // Cancel a task after it has already completed. cancel() should return false.
  private def demoCancelAfterCompletion(): Unit = {
    val pool = Executors.newSingleThreadExecutor(namedFactory("completed-demo"))
    try {
      val quick = new Callable[String] {
        override def call(): String = {
          println(s"[${Thread.currentThread.getName}] quick task started and will finish fast")
          Thread.sleep(100)
          println(s"[${Thread.currentThread.getName}] quick task finished")
          "quick-result"
        }
      }

      val qf = pool.submit(quick)
      // Wait until it completes
      val r = qf.get(1, TimeUnit.SECONDS)
      println(s"[main] qf.get() returned '$r' (isDone=${qf.isDone})")

      // Now try cancelling after completion
      val cancelAfter = qf.cancel(true)
      println(s"[main] qf.cancel(true) after completion returned $cancelAfter")
      println(s"[main] qf.isCancelled = ${qf.isCancelled}, qf.isDone = ${qf.isDone}")
    } finally {
      shutdownPool(pool)
    }
  }

  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val id = new AtomicInteger(1)
    override def newThread(r: Runnable): Thread = new Thread(r, s"$prefix-${id.getAndIncrement()}")
  }

  private def shutdownPool(pool: ExecutorService): Unit = {
    pool.shutdown()
    try {
      if (!pool.awaitTermination(3, TimeUnit.SECONDS)) pool.shutdownNow()
    } catch {
      case _: InterruptedException => pool.shutdownNow(); Thread.currentThread().interrupt()
    }
  }

  private def sep(): Unit = { println(); println() }
}

