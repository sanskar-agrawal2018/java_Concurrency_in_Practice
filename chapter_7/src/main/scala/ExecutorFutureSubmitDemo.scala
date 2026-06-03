/**
 * Chapter 7 -- Submitting Futures to an ExecutorService
 *
 * ExecutorService.submit() has three overloads:
 *
 *   submit(Runnable)            → Future[?]    result is always null
 *   submit(Runnable, T result)  → Future[T]    result is the value you pass in
 *   submit(Callable[T])         → Future[T]    result comes from Callable.call()
 *
 * Key Future methods:
 *   future.get()                -- blocks until done, returns result or throws
 *   future.get(timeout, unit)   -- same but with a deadline
 *   future.isDone               -- non-blocking poll
 *   future.cancel(mayInterrupt) -- request cancellation
 *   future.isCancelled          -- check if cancelled
 *
 * Demos in this file:
 *  1. submit(Callable)   -- most common; task returns a value
 *  2. submit(Runnable)   -- fire-and-forget; get() returns null
 *  3. submit(Runnable, result) -- pre-set result for synchronisation
 *  4. get() with timeout -- avoid blocking forever
 *  5. cancel(true)       -- cancel a slow future mid-flight
 *  6. invokeAll()        -- submit a batch and wait for every one
 *  7. invokeAny()        -- submit a batch and take the first result
 *
 * Run:
 *   sbt "chapter7/runMain ExecutorFutureSubmitDemo"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

object ExecutorFutureSubmitDemo {

  def main(args: Array[String]): Unit = {
    demo1_submitCallable()
    sep()
    demo2_submitRunnable()
    sep()
    demo3_submitRunnableWithResult()
    sep()
    demo4_getWithTimeout()
    sep()
    demo5_cancelFuture()
    sep()
    demo6_invokeAll()
    sep()
    demo7_invokeAny()
  }

  // ── Demo 1: submit(Callable) ────────────────────────────────────────────────
  private def demo1_submitCallable(): Unit = {
    println("DEMO 1: submit(Callable) -- task returns a computed value")
    println("=" * 70)

    val pool = fixedPool("callable", 3)
    try {
      // Submit three Callable tasks that each return a string result.
      val f1: Future[String] = pool.submit(callable("task-A", 300, "result-A"))
      val f2: Future[String] = pool.submit(callable("task-B", 100, "result-B"))
      val f3: Future[String] = pool.submit(callable("task-C", 200, "result-C"))

      // get() blocks until the future is done.
      // We call them in submission order -- f1 blocks longest, but f2 and f3
      // are already done by then, so their get() returns immediately after f1.
      println(s"[main] f1.get() = ${f1.get()}")
      println(s"[main] f2.get() = ${f2.get()}")
      println(s"[main] f3.get() = ${f3.get()}")
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 2: submit(Runnable) ────────────────────────────────────────────────
  private def demo2_submitRunnable(): Unit = {
    println("DEMO 2: submit(Runnable) -- fire-and-forget; get() returns null")
    println("=" * 70)

    val pool = fixedPool("runnable", 2)
    try {
      val counter = new AtomicInteger(0)

      val r: Runnable = () => {
        Thread.sleep(150)
        val n = counter.incrementAndGet()
        println(s"[${Thread.currentThread().getName}] Runnable ran; counter=$n")
      }

      val f1: Future[_] = pool.submit(r)
      val f2: Future[_] = pool.submit(r)

      // get() on a Runnable future always returns null -- its only use is to
      // block until the task completes (or detect a thrown exception).
      val result1 = f1.get()
      val result2 = f2.get()
      println(s"[main] f1.get() = $result1  (null is expected for Runnable)")
      println(s"[main] f2.get() = $result2")
      println(s"[main] final counter = ${counter.get()}")
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 3: submit(Runnable, T) ─────────────────────────────────────────────
  private def demo3_submitRunnableWithResult(): Unit = {
    println("DEMO 3: submit(Runnable, result) -- pre-set result for synchronisation")
    println("=" * 70)
    println("Useful when you only care whether the task finished, not what it computed.")
    println()

    val pool = fixedPool("runnableResult", 2)
    try {
      val sentinel = "DONE"   // the value get() will return once the task finishes

      val f: Future[String] = pool.submit(
        (() => {
          Thread.sleep(200)
          println(s"[${Thread.currentThread().getName}] Runnable with pre-set result finished")
        }): Runnable,
        sentinel
      )

      println(s"[main] isDone before get(): ${f.isDone}")
      val result = f.get()
      println(s"[main] f.get() = '$result'  (isDone=${f.isDone})")
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 4: get() with timeout ──────────────────────────────────────────────
  private def demo4_getWithTimeout(): Unit = {
    println("DEMO 4: get(timeout, unit) -- avoid blocking forever")
    println("=" * 70)

    val pool = fixedPool("timeout", 2)
    try {
      val fastFuture = pool.submit(callable("fast", 100, "fast-result"))
      val slowFuture = pool.submit(callable("slow", 2000, "slow-result"))

      // Fast task: timeout is generous, completes in time.
      try {
        val r = fastFuture.get(500, TimeUnit.MILLISECONDS)
        println(s"[main] fast result: $r")
      } catch {
        case _: TimeoutException => println("[main] fast task timed out -- unexpected")
      }

      // Slow task: timeout expires before the task finishes.
      try {
        val r = slowFuture.get(300, TimeUnit.MILLISECONDS)
        println(s"[main] slow result: $r -- unexpected")
      } catch {
        case _: TimeoutException =>
          println("[main] slow task timed out as expected; cancelling it")
          slowFuture.cancel(true)
          println(s"[main] slowFuture.isCancelled = ${slowFuture.isCancelled}")
      }
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 5: cancel(true) ────────────────────────────────────────────────────
  private def demo5_cancelFuture(): Unit = {
    println("DEMO 5: cancel(true) -- interrupt a running future mid-flight")
    println("=" * 70)

    val pool = fixedPool("cancel", 1)
    try {
      val processed = new AtomicInteger(0)

      val f: Future[String] = pool.submit(new Callable[String] {
        override def call(): String = {
          println(s"[${Thread.currentThread().getName}] long task started")
          try {
            while (processed.get() < 10) {
              Thread.sleep(200)
              val n = processed.incrementAndGet()
              println(s"[${Thread.currentThread().getName}] progress $n")
            }
          } catch {
            case _: InterruptedException =>
              println(s"[${Thread.currentThread().getName}] interrupted; stopping gracefully")
              Thread.currentThread().interrupt()  // restore flag
          }
          s"completed after ${processed.get()} steps"
        }
      })

      Thread.sleep(500)
      println(s"[main] cancelling future; isCancelled before = ${f.isCancelled}")
      val cancelled = f.cancel(true)
      println(s"[main] cancel(true) returned $cancelled")

      try {
        f.get(1, TimeUnit.SECONDS)
      } catch {
        case _: CancellationException =>
          println(s"[main] f.get() threw CancellationException as expected")
        case _: TimeoutException =>
          println(s"[main] get() timed out; task may not have stopped")
      }

      println(s"[main] steps processed before cancel: ${processed.get()}")
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 6: invokeAll() ─────────────────────────────────────────────────────
  private def demo6_invokeAll(): Unit = {
    println("DEMO 6: invokeAll() -- submit a batch; blocks until ALL are done")
    println("=" * 70)

    val pool = fixedPool("invokeAll", 3)
    try {
      import scala.jdk.CollectionConverters._

      val tasks: java.util.List[Callable[String]] = List(
        callable("batch-A", 300, "A"),
        callable("batch-B", 100, "B"),
        callable("batch-C", 200, "C")
      ).asJava

      // invokeAll blocks until every task is done (or the calling thread is interrupted).
      // All returned futures are guaranteed to be isDone == true on return.
      val futures: java.util.List[Future[String]] = pool.invokeAll(tasks)

      futures.asScala.zipWithIndex.foreach { case (f, i) =>
        // No risk of blocking here -- all futures are already done.
        println(s"[main] future[$i]: isDone=${f.isDone}  result=${f.get()}")
      }
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Demo 7: invokeAny() ─────────────────────────────────────────────────────
  private def demo7_invokeAny(): Unit = {
    println("DEMO 7: invokeAny() -- submit a batch; return the first successful result")
    println("=" * 70)
    println("Remaining tasks are cancelled once a winner is found.")
    println()

    val pool = fixedPool("invokeAny", 3)
    try {
      import scala.jdk.CollectionConverters._

      val tasks: java.util.List[Callable[String]] = List(
        callable("replica-slow",   900, "slow-value"),
        callable("replica-fast",   150, "fast-value"),
        callable("replica-medium", 500, "medium-value")
      ).asJava

      // invokeAny returns the result of whichever Callable finishes first.
      // If the first finisher throws, it tries the next, and so on.
      val winner: String = pool.invokeAny(tasks)
      println(s"[main] invokeAny winner: '$winner'")
    } finally {
      shutdownPool(pool)
    }
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private def callable(name: String, delayMs: Long, result: String): Callable[String] =
    new Callable[String] {
      override def call(): String = {
        println(s"[${Thread.currentThread().getName}] $name started (${delayMs}ms)")
        Thread.sleep(delayMs)
        println(s"[${Thread.currentThread().getName}] $name done -> '$result'")
        result
      }
    }

  private def fixedPool(prefix: String, size: Int): ExecutorService =
    Executors.newFixedThreadPool(size, new ThreadFactory {
      private val id = new AtomicInteger(1)
      override def newThread(r: Runnable) = new Thread(r, s"$prefix-${id.getAndIncrement()}")
    })

  private def shutdownPool(pool: ExecutorService): Unit = {
    pool.shutdown()
    if (!pool.awaitTermination(5, TimeUnit.SECONDS))
      pool.shutdownNow()
  }

  private def sep(): Unit = { println(); println() }
}
