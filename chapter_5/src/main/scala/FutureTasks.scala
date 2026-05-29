/*
 * FutureTask  (java.util.concurrent.FutureTask)
 *
 * FutureTask is the canonical "compute a value once, hand it to whoever asks"
 * synchronizer. It wraps a Callable[V] (or a Runnable + a fixed result), runs
 * it AT MOST ONCE, and exposes the outcome through Future[V]:
 *
 *   ft.run()           -- executes the Callable on the current thread. If the
 *                         task has already been started, this call is a no-op.
 *   ft.get()           -- blocks until the task finishes, then returns the
 *                         value. If the task threw, get() throws an
 *                         ExecutionException whose .getCause is the original.
 *   ft.get(t, unit)    -- same, but throws TimeoutException after t units.
 *   ft.cancel(mayInt)  -- abandons the task. If it has not started yet, it
 *                         never will. If it is running and mayInterrupt is
 *                         true, the running thread is interrupted.
 *   ft.isDone / isCancelled -- non-blocking status checks.
 *
 * Key guarantees:
 *
 *   - "Run at most once": two threads racing to call run() do not both
 *     execute the body; the loser sees the task transition past NEW and
 *     returns immediately.
 *   - "Result is published safely": whatever the Callable returns (or throws)
 *     happens-before any successful get(). No extra synchronization needed.
 *   - "Many readers, one writer": any number of threads may call get() and
 *     all see the same result.
 *
 * FutureTask is what ExecutorService.submit(...) actually returns internally,
 * but you can use it standalone -- e.g. to start a computation on a dedicated
 * Thread, or to memoize an expensive value (the classic Goetz "Memoizer"
 * pattern from JCiP, ch. 5).
 *
 * Important distinction:
 *
 *   This file demonstrates Java's java.util.concurrent.Future/FutureTask API,
 *   not scala.concurrent.Future. Java Future is mainly a blocking handle for a
 *   running computation. It does not have map/flatMap callbacks. You normally
 *   call get(), get(timeout), cancel(), isDone, or isCancelled on it.
 *
 * Syntax used in this file:
 *
 *   new Callable[Int] {
 *     def call(): Int = 42
 *   }
 *       A Callable[A] is a Java function object that returns a value of type A.
 *       FutureTask executes call() when the task runs.
 *
 *   val task = new FutureTask[Int](callable)
 *       Creates a FutureTask whose result type is Int. FutureTask implements
 *       both Runnable and Future[Int].
 *
 *   new Thread(task, "thread-name").start()
 *       Starts the FutureTask on a real JVM thread. Because FutureTask is a
 *       Runnable, it can be passed directly to Thread.
 *
 *   task.get()
 *       Blocks the caller until the background computation completes. It returns
 *       the Callable result or throws ExecutionException if call() failed.
 *
 *   task.get(100, TimeUnit.MILLISECONDS)
 *       Blocks for at most the given duration. If the result is not ready, it
 *       throws TimeoutException.
 *
 *   task.cancel(true)
 *       Attempts to cancel the task. The true flag means "interrupt the worker
 *       thread if the task is already running".
 *
 *   task.isDone
 *       Returns true when the task completed, failed, or was cancelled.
 *
 *   task.isCancelled
 *       Returns true when cancellation succeeded.
 *
 * Functions available in this object:
 *
 *   demoBasicGet()
 *       Starts one FutureTask on a worker thread, does other work on main, then
 *       uses get() to retrieve the computed result.
 *
 *   demoExceptionPropagation()
 *       Shows that exceptions thrown inside call() are stored by FutureTask and
 *       re-thrown from get() as ExecutionException.
 *
 *   demoTimedGet()
 *       Uses get(timeout, unit) to avoid waiting forever for a slow task.
 *
 *   demoCancel()
 *       Cancels a running task and shows how cancellation interrupts a sleeping
 *       worker when mayInterruptIfRunning is true.
 *
 *   demoRunAtMostOnce()
 *       Starts two threads with the same FutureTask. Only one thread executes
 *       call(), proving the FutureTask body runs at most once.
 */

import java.util.concurrent.{Callable, ExecutionException, FutureTask, TimeUnit, TimeoutException}

object FutureTasks {

  // -------------------------------------------------------------------------
  // Demo A -- compute a value on a background thread and pick it up later.
  // -------------------------------------------------------------------------
  private def demoBasicGet(): Unit = {
    println("[A] FutureTask: compute on a worker thread, retrieve via get()")
    // FutureTask[Int] means this task will eventually produce an Int.
    val task = new FutureTask[Int](new Callable[Int] {
      // call() is the body that FutureTask runs exactly once.
      def call(): Int = {
        Thread.sleep(200)                       // simulate expensive work
        42
      }
    })

    new Thread(task, "ft-worker-A").start()     // FutureTask IS a Runnable
    println("[A]   main is free to do other work while the task runs...")
    val value = task.get()                      // blocks until call() returns
    println(s"[A]   got value = $value, isDone=${task.isDone}")
  }

  // -------------------------------------------------------------------------
  // Demo B -- exceptions in the Callable are captured and re-thrown by get()
  // wrapped in an ExecutionException. The original is in .getCause.
  // -------------------------------------------------------------------------
  private def demoExceptionPropagation(): Unit = {
    println("[B] exceptions thrown in call() resurface from get() as ExecutionException")
    val task = new FutureTask[Int](new Callable[Int] {
      def call(): Int = throw new IllegalStateException("computation failed")
    })
    new Thread(task, "ft-worker-B").start()

    try task.get()
    catch {
      case e: ExecutionException =>
        // getCause gives access to the original exception thrown by call().
        println(s"[B]   get() threw ExecutionException; cause = ${e.getCause}")
    }
  }

  // -------------------------------------------------------------------------
  // Demo C -- get(timeout) lets the caller bound how long it will wait.
  // -------------------------------------------------------------------------
  private def demoTimedGet(): Unit = {
    println("[C] get(timeout, unit) throws TimeoutException if not ready in time")
    val task = new FutureTask[String](new Callable[String] {
      def call(): String = { Thread.sleep(500); "ready" }
    })
    new Thread(task, "ft-worker-C").start()

    try {
      // This waits only 100 ms. The task sleeps 500 ms, so TimeoutException is expected.
      val v = task.get(100, TimeUnit.MILLISECONDS)
      println(s"[C]   surprising: got $v in time")
    } catch {
      case _: TimeoutException =>
        println("[C]   gave up after 100ms; task is still running")
    }
    // tidy up so the JVM can exit promptly
    task.cancel(true)
  }

  // -------------------------------------------------------------------------
  // Demo D -- cancel() prevents a not-yet-started task from running and
  // interrupts a running one (when mayInterruptIfRunning is true).
  // -------------------------------------------------------------------------
  private def demoCancel(): Unit = {
    println("[D] cancel(true) interrupts the running thread and flips isCancelled")
    val task = new FutureTask[Int](new Callable[Int] {
      def call(): Int = {
        try { Thread.sleep(5000); 1 }
        catch {
          case _: InterruptedException =>
            println("[D]   call() saw the interrupt and bailed out")
            -1
        }
      }
    })
    new Thread(task, "ft-worker-D").start()
    Thread.sleep(100)
    // true means FutureTask may interrupt the running worker thread.
    val ok = task.cancel(true)                  // interrupts the worker
    println(s"[D]   cancel returned $ok, isCancelled=${task.isCancelled}")
  }

  // -------------------------------------------------------------------------
  // Demo E -- "run at most once": two starters race; only the first actually
  // executes the body. The other call() returns immediately.
  // -------------------------------------------------------------------------
  private def demoRunAtMostOnce(): Unit = {
    println("[E] FutureTask.run() executes the body AT MOST ONCE")
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val task = new FutureTask[Int](new Callable[Int] {
      def call(): Int = counter.incrementAndGet()
    })

    val r1 = new Thread(task, "ft-runner-1")
    val r2 = new Thread(task, "ft-runner-2")
    // Both threads receive the same FutureTask instance, but only one runs call().
    r1.start(); r2.start()
    r1.join();  r2.join()

    println(s"[E]   counter incremented ${counter.get} time(s); get() = ${task.get()}")
  }

  def main(args: Array[String]): Unit = {
    demoBasicGet();             println()
    demoExceptionPropagation(); println()
    demoTimedGet();             println()
    demoCancel();               println()
    demoRunAtMostOnce()
  }
}
