/**
 * Chapter 7 -- Thread pool interruption demo
 *
 * This example demonstrates two contrasting worker implementations to show
 * the consequences of mishandling Thread interruption vs. handling it
 * properly.
 *
 * Background / key points:
 * - ExecutorService.shutdownNow() and Future.cancel(true) do not forcibly
 *   terminate a thread; they only interrupt it. The task must cooperate by
 *   responding to InterruptedException or inspecting the interrupted flag.
 * - If task code swallows InterruptedException and continues, cancellation
 *   requests are ignored and the executor may continue mutating shared state
 *   even after shutdownNow() was called.
 * - Proper handling means treating interruption as a stop request: restore
 *   the interrupted flag if you catch InterruptedException and then exit.
 *
 * This file contains two demos:
 * 1) bad interruption handling -- worker swallows InterruptedException and
 *    continues processing; late work submitted after shutdownNow() may still
 *    be processed.
 * 2) proper interruption handling -- worker exits promptly when interrupted;
 *    late work remains unprocessed in the external queue.
 *
 * Usage (from project root):
 *   sbt "runMain ThreadPoolInterruptionDemo"
 *
 * The behavior is printed to stdout so you can inspect the order of events
 * and the counts of processed items.
 */

/**
 * Demonstration object. Contains two small demos and helper utilities.
 */

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

object ThreadPoolInterruptionDemo {
  private val InitialWork = "initial-work"
  private val LateWork1 = "late-work-after-cancel-1"
  private val LateWork2 = "late-work-after-cancel-2"

  def main(args: Array[String]): Unit = {
    demoBadInterruptionHandling()
    println()
    println("=" * 90)
    println()
    demoProperInterruptionHandling()
  }

  private def demoBadInterruptionHandling(): Unit = {
    println("DEMO 1: bad interruption handling")
    println("=" * 90)
    println("The worker catches InterruptedException, does not restore the flag, and continues.")
    println("Side effect: shutdownNow() cannot stop the task promptly, and late work still runs.")
    println()

    val queue: BlockingQueue[String] = new ArrayBlockingQueue[String](4)
    val processed = new AtomicInteger(0)
    val swallowedInterrupts = new AtomicInteger(0)
    val started = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(1, namedThreadFactory("bad-pool"))

    queue.put(InitialWork)
    executor.submit(badQueueConsumer(queue, processed, swallowedInterrupts, started))

    started.await()
    waitUntil("bad worker to process the first item", 1000L) {
      processed.get() >= 1
    }

    println()
    println("[main] Calling shutdownNow(). This interrupts the running worker.")
    val neverStarted = executor.shutdownNow()
    println(s"[main] shutdownNow returned ${neverStarted.size()} executor task(s) that never started")

    val stoppedQuickly = executor.awaitTermination(300, TimeUnit.MILLISECONDS)
    println(s"[main] awaitTermination(300ms) -> $stoppedQuickly")
    println(s"[main] processed count after cancellation request -> ${processed.get()}")
    println(s"[main] swallowed interrupts -> ${swallowedInterrupts.get()}")

    println()
    println("[main] Adding more items to the external queue after cancellation was requested.")
    println("[main] A correctly interrupted worker would not process these.")
    queue.put(LateWork1)
    queue.put(LateWork2)

    val stoppedEventually = executor.awaitTermination(2, TimeUnit.SECONDS)
    println(s"[main] awaitTermination(after feeding late work) -> $stoppedEventually")
    println(s"[main] final processed count -> ${processed.get()}")
    println("[main] Side effect shown: the bad worker mutated state after shutdownNow().")
  }

  private def demoProperInterruptionHandling(): Unit = {
    println("DEMO 2: proper interruption handling")
    println("=" * 90)
    println("The worker treats interruption as a stop request, restores the flag, and exits.")
    println("Side effect avoided: late work remains in the queue and shared state stops changing.")
    println()

    val queue: BlockingQueue[String] = new ArrayBlockingQueue[String](4)
    val processed = new AtomicInteger(0)
    val started = new CountDownLatch(1)
    val exited = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(1, namedThreadFactory("good-pool"))

    queue.put(InitialWork)
    executor.submit(properQueueConsumer(queue, processed, started, exited))

    started.await()
    waitUntil("proper worker to process the first item", 1000L) {
      processed.get() >= 1
    }

    println("")
    println("[main] Calling shutdownNow(). This interrupts the running worker.")
    val neverStarted = executor.shutdownNow()
    println(s"[main] shutdownNow returned ${neverStarted.size()} executor task(s) that never started")

    val stopped = executor.awaitTermination(2, TimeUnit.SECONDS)
    println(s"[main] awaitTermination(2s) -> $stopped")
    println(s"[main] worker cleanup observed -> ${exited.await(0, TimeUnit.MILLISECONDS)}")

    println()
    println("[main] Adding late work after the pool has stopped.")
    queue.put(LateWork1)
    Thread.sleep(200)
    println(s"[main] final processed count -> ${processed.get()}")
    println(s"[main] remaining items in queue -> ${queue.size()}")
    println("[main] Proper handling shown: cancellation stopped the worker before late work ran.")
  }

  private def badQueueConsumer(
      queue: BlockingQueue[String],
      processed: AtomicInteger,
      swallowedInterrupts: AtomicInteger,
      started: CountDownLatch
  ): Runnable = new Runnable {
    override def run(): Unit = {
      started.countDown()
//      var keepRunning: AtomicBoolean = new AtomicBoolean(true)
      while (processed.get() < 3) {
        try {
          val item = queue.take()
          val count = processed.incrementAndGet()
          println(s"[${Thread.currentThread().getName}] processed $item; sideEffectCount=$count")
          Thread.sleep(120)
        } catch {
          case _: InterruptedException =>
            val count = swallowedInterrupts.incrementAndGet()
            println(
              s"[${Thread.currentThread().getName}] swallowed interrupt #$count; " +
                s"isInterrupted=${Thread.currentThread().isInterrupted}"
            )
            println(s"[${Thread.currentThread().getName}] continuing anyway -- this is the bug")
        }
      }

      println(s"[${Thread.currentThread().getName}] bad worker exits only because this demo limits side effects")
    }
  }

  private def properQueueConsumer(
      queue: BlockingQueue[String],
      processed: AtomicInteger,
      started: CountDownLatch,
      exited: CountDownLatch
  ): Runnable = new Runnable {
    override def run(): Unit = {
      started.countDown()
      var keepRunning = true

      try {
        while (keepRunning) {
          try {
            val item = queue.take()
            val count = processed.incrementAndGet()
            println(s"[${Thread.currentThread().getName}] processed $item; sideEffectCount=$count")
            Thread.sleep(120)
          } catch {
            case _: InterruptedException =>
              println(s"[${Thread.currentThread().getName}] IsInterrupted flag :-> ${Thread.currentThread().isInterrupted}")
              println(s"[${Thread.currentThread().getName}] interrupted; restoring flag and leaving loop")

              Thread.currentThread().interrupt()
              println(s"[${Thread.currentThread().getName}] after restoring flag: isInterrupted=${Thread.currentThread().isInterrupted}")
              keepRunning = false
          }
        }
      } finally {
        // we can do cleanup here if needed, but we should not mutate shared state that the main thread is waiting on
        println(
          s"[${Thread.currentThread().getName}] cleanup runs; " +
            s"isInterrupted=${Thread.currentThread().isInterrupted}"
        )
        exited.countDown()
      }
    }
  }

  private def waitUntil(description: String, timeoutMillis: Long)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)

    while (!condition && System.nanoTime() < deadline) {
      try Thread.sleep(10)
      catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new RuntimeException(s"Interrupted while waiting for $description")
      }
    }

    if (!condition) {
      println(s"[main] timed out waiting for $description; continuing so the demo can show current state")
    }
  }

  private def namedThreadFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextId.getAndIncrement()}")
  }
}
