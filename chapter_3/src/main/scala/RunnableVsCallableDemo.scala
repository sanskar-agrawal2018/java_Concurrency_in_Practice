/**
 * Chapter 3 - Runnable vs Callable
 *
 * Runnable is the older "run some work" interface:
 *   - method: run(): Unit
 *   - result: no direct return value
 *   - usage: can be passed directly to Thread or ExecutorService
 *   - error/result handling: callers usually need shared state, logging,
 *     queues, or a Future returned by an ExecutorService
 *
 * Callable[A] is the "compute a value" interface:
 *   - method: call(): A
 *   - result: returns a value of type A
 *   - usage: can be submitted to ExecutorService, or wrapped in FutureTask
 *     when you need to run it on a plain Thread
 *   - error/result handling: Future.get() returns the value or throws an
 *     ExecutionException whose cause is the original failure
 *
 * Practical rule:
 *   - Use Runnable for fire-and-forget side effects.
 *   - Use Callable[A] when the task naturally produces a result or failure
 *     that the caller must observe.
 *
 * Run:
 *   sbt "chapter3/runMain RunnableVsCallableDemo"
 */

import java.util.concurrent.{
  Callable,
  ExecutionException,
  ExecutorService,
  Executors,
  Future,
  FutureTask,
  ThreadFactory,
  TimeUnit,
  ExecutorCompletionService
}
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger

object RunnableVsCallableDemo {

  def main(args: Array[String]): Unit = {
    demoRunnableOnThread()
    separator()
    demoExecutorSubmitRunnable()
    separator()
    demoExecutorSubmitCallable()
    separator()
    demoCallableWithFutureTask()
    separator()
    demoCompletionService()
  }

  private def demoRunnableOnThread(): Unit = {
    println("DEMO 1: Runnable with Thread - work completes, but returns no value")
    println("=" * 78)

    val sharedCounter = new AtomicInteger(0)

    val runnable: Runnable = () => {
      Thread.sleep(100)
      val value = sharedCounter.incrementAndGet()
      println(s"[${Thread.currentThread().getName}] run() finished; sharedCounter=$value")
    }

    val thread = new Thread(runnable, "plain-runnable-thread")
    thread.start()

    // join() only waits for completion. It does not give back a computed value.
    thread.join()
    println(s"[main] Thread.join() completed; result must come from shared state: ${sharedCounter.get()}")
  }

  private def demoExecutorSubmitRunnable(): Unit = {
    println("DEMO 2: submit(Runnable) - Future.get() waits, then returns null")
    println("=" * 78)

    val pool = singleThreadExecutor("executor-runnable-thread")
    try {
      val runnable: Runnable = () => {
        Thread.sleep(100)
        println(s"[${Thread.currentThread().getName}] Runnable side effect completed")
      }

      val future: Future[_] = pool.submit(runnable)

      // The Future is useful as a completion/failure handle, not as a value handle.
      val result = future.get()
      println(s"[main] future.get() from submit(Runnable) = $result")
      println("[main] null is expected because Runnable.run() has no return value")
    } finally {
      shutdown(pool)
    }
  }

  private def demoExecutorSubmitCallable(): Unit = {
    println("DEMO 3: submit(Callable[A]) - Future.get() returns the computed value")
    println("=" * 78)

    val pool = singleThreadExecutor("executor-callable-thread")
    try {
      val callable: Callable[String] = new Callable[String] {
        override def call(): String = {
          Thread.sleep(100)
          s"computed by ${Thread.currentThread().getName}"
        }
      }

      val future: Future[String] = pool.submit(callable)
      println(s"[main] future.get() from submit(Callable) = '${future.get()}'")

      val failingTask: Future[Int] = pool.submit(new Callable[Int] {
        override def call(): Int =
          throw new IllegalStateException("Callable failure is captured by Future")
      })

      try {
        failingTask.get()
      } catch {
        case e: ExecutionException =>
          println(s"[main] failed Callable came back through ExecutionException: ${e.getCause}")
      }
    } finally {
      shutdown(pool)
    }
  }

  private def demoCallableWithFutureTask(): Unit = {
    println("DEMO 4: FutureTask adapts Callable so it can run on a Thread")
    println("=" * 78)

    val callable: Callable[Int] = new Callable[Int] {
      override def call(): Int = {
        Thread.sleep(100)
        42
      }
    }

    // new Thread(callable) does not compile because Thread expects Runnable.
    // FutureTask implements both Runnable and Future, so it bridges that gap.
    val task = new FutureTask[Int](callable)
    val thread = new Thread(task, "future-task-thread")

    thread.start()
    println(s"[main] FutureTask.get() returned ${task.get()}")
  }

  /**
   * DEMO 5: submit a list of Callables to an Executor and use
   * ExecutorCompletionService to collect results as they complete.
   * We attach a human-readable name to each submitted Future via a map
   * so we can print "future name -> returned value" in completion order.
   */
  private def demoCompletionService(): Unit = {
    println("DEMO 5: CompletionService with named futures")
    println("=" * 78)

    val pool = Executors.newFixedThreadPool(4, new ThreadFactory {
      private val n = new AtomicInteger(0)
      override def newThread(r: Runnable): Thread = new Thread(r, s"cs-worker-${n.getAndIncrement()}")
    })

    try {
      val ecs = new ExecutorCompletionService[Int](pool)
      // Map the Future returned by submit(...) to a human name so we can
      // report which logical task completed when we call take().
      val nameByFuture = mutable.Map.empty[Future[Int], String]

      val tasks = (1 to 6).map { i =>
        new Callable[Int] {
          override def call(): Int = {
            // staggered sleep to simulate different durations
            Thread.sleep(100L * i)
            val value = i * 10
            println(s"[task-$i][${Thread.currentThread().getName}] computed $value")
            value
          }
        }
      }

      // submit tasks and record the mapping from Future->name
      tasks.zipWithIndex.foreach { case (c, idx) =>
        val name = s"task-${idx + 1}"
        val f = ecs.submit(c)
        nameByFuture.put(f, name)
      }

      // collect results as they complete and accumulate into a list
      var collected = List.empty[Int]
      for (_ <- tasks.indices) {
        val f = ecs.take() // blocks until the next task finishes
        val v = f.get()
        val n = nameByFuture.getOrElse(f, "<unknown>")
        println(s"[main] completed $n -> $v")
        collected ::= v
      }

      val total = collected.sum
      println(s"[main] CompletionService collected values (sum=$total): ${collected.reverse.mkString(",")}")
    } finally {
      shutdown(pool)
    }
  }

  private def singleThreadExecutor(threadName: String): ExecutorService =
    Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread =
        new Thread(r, threadName)
    })

  private def shutdown(pool: ExecutorService): Unit = {
    pool.shutdown()
    try {
      if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
        pool.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        pool.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  private def separator(): Unit = {
    println()
  }
}
