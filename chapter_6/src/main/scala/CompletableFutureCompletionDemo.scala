/*
 * CompletableFuture as a CompletionService Demo
 *
 * Java's ExecutorCompletionService uses a blocking queue to deliver
 * completed Future results in arrival order.  CompletableFuture achieves
 * the same by attaching callbacks that fire on task completion, so the
 * caller never blocks waiting for a specific future -- it just waits for
 * *any* completion.
 *
 * The three demos below mirror ExecutorCompletionServiceDemo but replace
 * ExecutorCompletionService with CompletableFuture pipelines:
 *
 *   Demo 1 -- collect results in completion order via a shared queue fed
 *             by thenAccept callbacks.
 *   Demo 2 -- handle failures inline with handle() / exceptionally().
 *   Demo 3 -- race N futures and take the first successful result
 *             with a custom raceFirst helper; cancel the rest.
 *
 * Run:
 *   sbt "chapter6/runMain CompletableFutureCompletionDemo"
 */

import java.util.concurrent.{CompletableFuture, Executors, LinkedBlockingQueue, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success, Try}

object CompletableFutureCompletionDemo {

  def main(args: Array[String]): Unit = {
    demoCompletionOrder()
    println()
    println("=" * 85)
    println()
    demoFailureHandling()
    println()
    println("=" * 85)
    println()
    demoRaceFirstSuccess()
  }

  // -----------------------------------------------------------------------
  // Demo 1 -- collect results in completion order
  // -----------------------------------------------------------------------
  private def demoCompletionOrder(): Unit = {
    println("DEMO 1: collect CompletableFuture results in completion order")
    println("=" * 85)

    val pool = Executors.newFixedThreadPool(3, namedFactory("cf-worker"))

    // A shared blocking queue acts as the "completion queue" -- each future
    // pushes its result here via thenAccept when it finishes.
    val completionQueue = new LinkedBlockingQueue[WorkResult]()

    val tasks = List(
      Work("slow-report",    900),
      Work("fast-cache",     200),
      Work("medium-api",     500),
      Work("tiny-metadata",  100),
      Work("batch-export",   700)
    )

    try {
      // Submit and register a callback that enqueues the result.
      val futures = tasks.map { work =>
        val cf = CompletableFuture.supplyAsync(
          () => {
            println(s"[${Thread.currentThread().getName}] starting ${work.name} (${work.delayMillis}ms)")
            cfSleep(work.delayMillis)
            WorkResult(work.name, work.delayMillis, Thread.currentThread().getName)
          },
          pool
        )
        cf.thenAccept(result => completionQueue.put(result))
        cf
      }

      println()
      println("Reading from completionQueue.take() as futures complete:")
      tasks.indices.foreach { _ =>
        val result = completionQueue.take()
        println(
          s"received ${result.name} after ${result.delayMillis}ms " +
          s"from ${result.threadName}"
        )
      }

      // Surface any exceptions that may have been silently swallowed.
      futures.foreach(f => f.getNow(null))  // no-op if already done
    } finally {
      shutdownPool(pool)
    }
  }

  // -----------------------------------------------------------------------
  // Demo 2 -- handle task failures as completed futures arrive
  // -----------------------------------------------------------------------
  private def demoFailureHandling(): Unit = {
    println("DEMO 2: handle failures with CompletableFuture.handle()")
    println("=" * 85)

    val pool = Executors.newFixedThreadPool(3, namedFactory("cf-failure"))

    // Queue holds Either-like wrappers: Right = success, Left = failure message.
    val completionQueue = new LinkedBlockingQueue[Try[String]]()

    try {
      def enqueue(name: String, delayMs: Long, shouldFail: Boolean): CompletableFuture[String] = {
        CompletableFuture
          .supplyAsync(
            () => {
              cfSleep(delayMs)
              if (shouldFail) throw new IllegalStateException(s"$name failed")
              else s"$name completed on ${Thread.currentThread().getName}"
            },
            pool
          )
          .handle[String] { (result, ex) =>
            // handle() is called whether the stage succeeded or failed.
            val outcome: Try[String] =
              if (ex != null) Failure(ex.getCause)   // CompletionException wraps; getCause is the real one
              else Success(result)
            completionQueue.put(outcome)
            result // return value is ignored here; we only care about the side effect
          }
      }

      val submittedCount = 4
      enqueue("load-users",     250, shouldFail = false)
      enqueue("load-payments",  150, shouldFail = true)
      enqueue("load-products",  100, shouldFail = false)
      enqueue("load-inventory", 300, shouldFail = true)

      (1 to submittedCount).foreach { _ =>
        completionQueue.take() match {
          case Success(msg) => println(s"success: $msg")
          case Failure(ex)  => println(s"failure: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        }
      }
    } finally {
      shutdownPool(pool)
    }
  }

  // -----------------------------------------------------------------------
  // Demo 3 -- race N futures; take the first successful result
  // -----------------------------------------------------------------------
  private def demoRaceFirstSuccess(): Unit = {
    println("DEMO 3: race futures and take the first successful result")
    println("=" * 85)

    val pool = Executors.newFixedThreadPool(3, namedFactory("cf-replica"))

    try {
      // Build the raw computation futures so we can cancel them later.
      val rawFutures: List[CompletableFuture[String]] = List(
        asyncTask(pool, "primary",   300, shouldFail = true),
        asyncTask(pool, "secondary", 500, shouldFail = false),
        asyncTask(pool, "backup",    900, shouldFail = false)
      )

      val result = raceFirstSuccess(rawFutures)
      println(s"first successful result: $result")
    } finally {
      shutdownPool(pool)
    }
  }

  /**
   * Returns the value of the first future that completes successfully.
   * All other futures are cancelled (best-effort) once a winner is found.
   * Throws RuntimeException if every future fails.
   */
  private def raceFirstSuccess(futures: List[CompletableFuture[String]]): String = {
    // A promise that will be completed by whichever task wins.
    val winner = new CompletableFuture[String]()

    futures.foreach { cf =>
      cf.handle[Unit] { (result, ex) =>
        if (ex == null) {
          // First successful completion wins the race.
          winner.complete(result)
        } else if (!winner.isDone) {
          val cause = if (ex.getCause != null) ex.getCause else ex
          println(s"ignoring failed attempt: ${cause.getMessage}")
          // If all futures have failed, complete exceptionally.
          if (futures.forall(_.isCompletedExceptionally)) {
            winner.completeExceptionally(new RuntimeException("all attempts failed", cause))
          }
        }
        ()
      }
    }

    try {
      winner.get()
    } catch {
      case e: java.util.concurrent.ExecutionException =>
        throw new RuntimeException(e.getCause)
    } finally {
      // Cancel remaining in-flight futures.
      futures.foreach(_.cancel(true))
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private def asyncTask(
    pool: java.util.concurrent.ExecutorService,
    name: String,
    delayMs: Long,
    shouldFail: Boolean
  ): CompletableFuture[String] =
    CompletableFuture.supplyAsync(
      () => {
        cfSleep(delayMs)
        if (shouldFail) throw new IllegalStateException(s"$name failed")
        else s"$name completed on ${Thread.currentThread().getName}"
      },
      pool
    )

  private def cfSleep(millis: Long): Unit =
    try Thread.sleep(millis)
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("task interrupted")
    }

  private def shutdownPool(pool: java.util.concurrent.ExecutorService): Unit = {
    pool.shutdown()
    if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
      pool.shutdownNow()
    }
  }

  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val id = new AtomicInteger(1)
    override def newThread(r: Runnable): Thread =
      new Thread(r, s"$prefix-${id.getAndIncrement()}")
  }
}
