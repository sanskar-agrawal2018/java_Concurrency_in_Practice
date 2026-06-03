/**
 * Chapter 7 -- Thread and Future interruption demos
 *
 * This file demonstrates:
 *  1) how a thread-based worker can mishandle InterruptedException and
 *     continue running (bad), and how to properly treat interruption as a
 *     stop request (good);
 *  2) cancelling a java.util.concurrent.Future with `cancel(true)` which
 *     interrupts the executing thread, and how the task should react.
 *
 * Key takeaways:
 * - interrupt is cooperative: callers can request interruption, but tasks
 *   must observe InterruptedException or Thread.interrupted() and stop.
 * - java.util.concurrent.Future.cancel(true) will interrupt the running
 *   thread; if the task swallows the exception it may keep running.
 *
 * Run this demo from project root:
 *   sbt "runMain ThreadFutureInterruptionDemo"
 */

import java.util.concurrent.{Callable, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object ThreadFutureInterruptionDemo {
  def main(args: Array[String]): Unit = {
    println("=== THREAD INTERRUPTION: bad vs good ===")
    threadInterruptionDemo()

    println()
    println("=== FUTURE CANCELLATION DEMO ===")
//    futureCancellationDemo()
  }

  // Part 1: show a thread worker that mishandles interruption vs one that
  // properly exits when interrupted.
  private def threadInterruptionDemo(): Unit = {
    val executor = Executors.newFixedThreadPool(2, namedFactory("thread-demo"))
    try {
      val bad = new Runnable {
        override def run(): Unit = {
          println(s"[${Thread.currentThread().getName}][Bad] bad worker started:-")
          var i = 0

          while (!Thread.currentThread().isInterrupted() && i < 5) {
            try {
              println(s"[${Thread.currentThread().getName}][Bad][i:$i] bad worker interrupted flag after swallowing: ${Thread.currentThread().isInterrupted()}")
              Thread.sleep(300) // simulate work
              val x= System.currentTimeMillis()

              i += 1
              println(s"[${Thread.currentThread().getName}][Bad] bad worker did work #$i")
            } catch {
              case _: InterruptedException =>
                // BUG: swallow the interrupt and continue instead of exiting
                Thread.currentThread().interrupt()
                println(s"[${Thread.currentThread().getName}][Bad] bad worker swallowed interrupt and continues")
                println(s"[${Thread.currentThread().getName}][Bad] bad worker interrupted flag after swallowing: ${Thread.currentThread().isInterrupted()}")
            }
          }
          println(s"[${Thread.currentThread().getName}][Bad] bad worker finished normally")
        }
      }

      val good = new Runnable {
        override def run(): Unit = {
          println(s"[${Thread.currentThread().getName}][Good] good worker started:-")
          var keepRunning = true
          var i = 0
          while (keepRunning && i < 10) {
            try {
              println(s"[${Thread.currentThread().getName}][Good][i:$i] good worker interrupted flag: ${Thread.currentThread().isInterrupted()}")
              Thread.sleep(300)
              i += 1
              println(s"[${Thread.currentThread().getName}][Good] good worker did work #$i")
            } catch {
              case _: InterruptedException =>
                // proper handling: restore flag and break out to stop
                println(s"[${Thread.currentThread().getName}][Good] good worker observed interrupt; stopping")
                Thread.currentThread().interrupt()
                println(s"[${Thread.currentThread().getName}][Good] good worker interrupted flag after swallowing: ${Thread.currentThread().isInterrupted()}")
                keepRunning = false
            }
          }
          println(s"[${Thread.currentThread().getName}][Good] good worker exiting; interrupted=${Thread.currentThread().isInterrupted()}")
        }
      }



      val goetzLowTask = new Runnable {
        override def run(): Unit = {
          try {
            println(s"[${Thread.currentThread().getName}][GoetzLaw] Print Value of interrupt flag: ${Thread.currentThread().isInterrupted()}")
            println(s"[${Thread.currentThread().getName}][GoetzLaw] GoetzLawTask started:-")
            // Sleep forever so that interrupted with shutDown
            Thread.sleep(10000)
          }
          catch {
            case _: InterruptedException => {
              Thread.currentThread().interrupt()
              println(s"[${Thread.currentThread().getName}][GoetzLaw] GoetzLawTask interrupted; exiting")
              println(s"[${Thread.currentThread().getName}][GoetzLaw] Print Value of interrupt flag: ${Thread.currentThread().isInterrupted()}")

            }
          }
        }
      }
      // Start both workers
      val badFuture = executor.submit(bad)
      val goodFuture = executor.submit(good)

      val goetzLowFuture = try {
          executor.submit(goetzLowTask)

      }
      badFuture.cancel(true)
      goodFuture.cancel(true)
      try {
        badFuture.get()

      } catch {
        case _: InterruptedException =>
          println(s"[${Thread.currentThread().getName}] Interrupted while waiting for badFuture")
        case _: java.util.concurrent.CancellationException =>
          println(s"[${Thread.currentThread().getName}] badFuture was cancelled")
      }


      // Let them run a bit
      Thread.sleep(500)
      println("[main] Requesting shutdownNow() on executor (this interrupts worker threads)")




//      val notStarted = executor.shutdownNow()
//      println(s"[main] shutdownNow returned ${notStarted.size()} tasks that never started:-")

      // Wait briefly to observe behavior
      executor.awaitTermination(2, TimeUnit.SECONDS)
      println("[main] after shutdownNow: both threads should have been interrupted; the bad worker may still have continued")
    } finally {
      if (!executor.isShutdown)
      {
        println("[main] Executor not shutdown yet; calling shutdownNow() to ensure JVM can exit")
        executor.shutdownNow()
      }

    }
  }

  // Part 2: demonstrate cancelling a java.util.concurrent.Future
  private def futureCancellationDemo(): Unit = {
    val executor = Executors.newSingleThreadExecutor(namedFactory("future-demo"))
    try {
      val processed = new AtomicInteger(0)

      val longRunning: Callable[Unit] = new Callable[Unit] {
        override def call(): Unit = {
          println(s"[${Thread.currentThread().getName}] future task started:-")
          try {
            // Simulate long-running work that checks interruption while sleeping
            while (processed.get() < 10) {
              Thread.sleep(250)
              val c = processed.incrementAndGet()
              println(s"[${Thread.currentThread().getName}] future task progress $c")
            }
          } catch {
            case _: InterruptedException =>
              // Proper handling: stop work and (optionally) restore the flag
              println(s"[${Thread.currentThread().getName}] future task interrupted; exiting")
              Thread.currentThread().interrupt()
          }
          println(s"[${Thread.currentThread().getName}] future task finished loop; processed=${processed.get()}")
        }
      }

      val future = executor.submit(longRunning)

      // Let task run a little
      Thread.sleep(700)
      println("[main] Cancelling future with cancel(true) to interrupt underlying thread")
      val cancelled = future.cancel(true)
      println(s"[main] future.cancel(true) returned $cancelled")

      // If the task handled interruption properly it should stop quickly; otherwise it may continue
      executor.awaitTermination(2, TimeUnit.SECONDS)
      println(s"[main] after cancel: processed=${processed.get()}")
    } finally {
      executor.shutdownNow()
    }
  }

  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val id = new AtomicInteger(1)
    override def newThread(r: Runnable): Thread = new Thread(r, s"$prefix-${id.getAndIncrement()}")
  }
}

