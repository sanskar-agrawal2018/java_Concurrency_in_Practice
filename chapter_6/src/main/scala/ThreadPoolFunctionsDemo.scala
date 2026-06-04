/**
 * Chapter 6 - Thread pool API tour
 *
 * This comprehensive demo illustrates the practical functions available on a Java thread pool,
 * organized into two levels of abstraction:
 *
 * === LEVEL 1: ExecutorService (generic task submission & lifecycle) ===
 *   - execute(Runnable)                      // Fire-and-forget, no result, no Future
 *   - submit(Runnable)                       // Returns Future[Unit]; blocks if queue full
 *   - submit(Runnable, result)               // Returns Future[T]; result is preset
 *   - submit(Callable[A])                    // Returns Future[A]; result comes from task
 *   - invokeAll(tasks)                       // Submit many, wait for ALL to complete
 *   - invokeAll(tasks, timeout, unit)        // With timeout; unfinished tasks are cancelled
 *   - invokeAny(tasks)                       // Submit many, return FIRST successful result
 *   - invokeAny(tasks, timeout, unit)        // With timeout; throws TimeoutException if none finish
 *   - shutdown()                             // Graceful: no new tasks, wait for existing ones
 *   - shutdownNow()                          // Abrupt: cancel queued tasks, interrupt running ones
 *   - awaitTermination(timeout, unit)        // Block until all tasks end or timeout expires
 *   - isShutdown()                           // True after shutdown() is called
 *   - isTerminated()                         // True after all tasks have completed
 *
 * === LEVEL 2: ThreadPoolExecutor (pool tuning & monitoring) ===
 *   - getCorePoolSize / setCorePoolSize      // Minimum threads kept alive even if idle
 *   - getMaximumPoolSize / setMaximumPoolSize // Maximum threads allowed
 *   - getKeepAliveTime / setKeepAliveTime    // How long non-core threads wait before exiting
 *   - allowCoreThreadTimeOut(...)/allows... // Allow core threads to exit when idle
 *   - prestartCoreThread / prestartAllCoreThreads // Create threads proactively
 *   - getPoolSize                            // Current number of threads in the pool
 *   - getActiveCount                         // Threads currently executing tasks
 *   - getLargestPoolSize                     // Peak number of threads that have existed
 *   - getTaskCount / getCompletedTaskCount   // Approximate total / completed task counts
 *   - getQueue                               // Access to the underlying task queue
 *   - remove(Runnable)                       // Remove a queued task before it runs
 *   - purge()                                // Remove all cancelled tasks from the queue
 *   - getThreadFactory / setThreadFactory    // Customize thread creation (names, daemons, etc.)
 *   - getRejectedExecutionHandler / setRejectedExecutionHandler  // Custom behavior when queue is full
 *   - isTerminating                          // True if shutdown() was called but tasks still running
 *   - toString()                             // Human-readable pool state snapshot
 *
 * Each demo section illustrates one or more of these functions in realistic scenarios.
 *
 * Run:
 *   sbt "chapter6/runMain ThreadPoolFunctionsDemo"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

object ThreadPoolFunctionsDemo {

  def main(args: Array[String]): Unit = {
//    section(1, "execute and submit")
//    demoExecuteAndSubmit()
//
//    section(2, "invokeAll and invokeAny")
//    demoInvokeFunctions()
//
//    section(3, "pool sizing and monitoring")
//    demoSizingAndMonitoring()
//
//    section(4, "prestart, thread factory, and keep-alive")
//    demoPrestartFactoryAndKeepAlive()
//
//    section(5, "remove and purge queued tasks")
//    demoRemoveAndPurge()
//
//    section(6, "rejection handler")
//    demoRejectionHandler()
//
//    section(7, "shutdown lifecycle")
//    demoShutdownLifecycle()
//    section(8, "task and thread states demo")
//    demoTaskAndThreadStates()

    // Note: Demo 9 (executor queues) has been extracted to ExecutorQueuesDemo.scala
    // Run it with: sbt "chapter6/runMain ExecutorQueuesDemo"
  }

  /**
   * DEMO 9: Different queue types for ThreadPoolExecutor
   *
   * This demo has been extracted to ExecutorQueuesDemo.scala for better organization.
   * Run it with: sbt "chapter6/runMain ExecutorQueuesDemo"
   */


  private def demoTaskAndThreadStates(): Unit = {
    println("DEMO 8: Thread states (NEW, BLOCKED, RUNNABLE, TERMINATED) and Future/task states")
//    println("=" * 78)
//
//    // ----- Thread state demonstration (NEW -> BLOCKED -> RUNNABLE -> TERMINATED) -----
//    val lock = new Object
//
//    val holder = new Thread(() => {
//      lock.synchronized {
//        println(s"[holder] holding lock (thread state=${Thread.currentThread().getState})")
//        Thread.sleep(300) // hold the lock long enough for another thread to block
//        println("[holder] releasing lock")
//      }
//    }, "holder-thread")
//
//    val blocker = new Thread(() => {
//      // when this thread tries to acquire the lock it will be BLOCKED until holder releases
//      lock.synchronized {
//        println(s"[blocker] acquired lock (inside synchronized)")
//      }
//    }, "blocker-thread")
//
//    println(s"[main] blocker state before start = ${blocker.getState}") // NEW
//    holder.start()
//    Thread.sleep(20) // ensure holder acquires the lock first
//    blocker.start()
//    Thread.sleep(20) // give blocker a moment to attempt to enter synchronized
//    println(s"[main] blocker state while blocked   = ${blocker.getState} (expected BLOCKED)")
//    holder.join()
//    Thread.sleep(20)
//    println(s"[main] blocker state after holder releases = ${blocker.getState}")
//    blocker.join()
//    println(s"[main] blocker state after join    = ${blocker.getState} (expected TERMINATED)")

    // ----- Future / Executor task state demonstration (queued -> running -> done/cancelled) -----
    val pool = new ThreadPoolExecutor(
      1, 1, 0L, TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue[Runnable](1), // small queue to force queuing
      namedFactory("state-demo")
    )

    try {
      val runningLatch = new CountDownLatch(1)

      val longTask = new Callable[String] {
        override def call(): String = {
          try {
            // signal we have started running
            runningLatch.countDown()
            println(s"[longTask][${Thread.currentThread().getName}] started and running")
            // do some work in a loop so cancellation/interruption can be observed
            var i = 0
            while (i < 10) {
              println(s"[longTask][${Thread.currentThread().getName}] Went to sleep for iteration $i")
              Thread.sleep(100)

              println(s"[longTask][${Thread.currentThread().getName}] Went to sleep for iteration $i")
              i += 1
            }
            "long-done"
          } catch {
            case _: InterruptedException =>
              println(s"[longTask][${Thread.currentThread().getName}] interrupted")
              "long-interrupted"
          }
        }
      }

      val shortTask = new Callable[String] {
        override def call(): String = {
          println(s"[shortTask][${Thread.currentThread().getName}] running")
          "short-done"
        }
      }

      // Submit longTask -> occupies the single worker thread
      val f1 = pool.submit(longTask)
      // Wait until the longTask has actually started running
      runningLatch.await()

      // Submit shortTask -> should be queued (queue capacity = 1)
      val f2 = pool.submit(shortTask)

      try {
        val f3= pool.submit(shortTask)
      }
      catch {
        case e: RejectedExecutionException =>
          println(s"[main] submission of f3 was rejected as expected: $e")
      }
     // This will be rejected due to queue capacity and AbortPolicy



      println(s"[main] after submit: pool queue size = ${pool.getQueue.size()}, f1.isDone=${f1.isDone}, f2.isDone=${f2.isDone}, f2.isCancelled=${f2.isCancelled}")
      Thread.sleep(50) // give a moment for the shortTask to be picked up if it wasn't cancelled
      println(s"[main] after submit: pool queue size = ${pool.getQueue.size()}, f1.isDone=${f1.isDone}, f2.isDone=${f2.isDone}, f2.isCancelled=${f2.isCancelled}")

      // Cancel the queued future softly (don't interrupt if running)
      val cancelled = f2.cancel(false)
      println(s"[main] cancelled queued future f2 with cancel(false) -> returned $cancelled; f2.isCancelled=${f2.isCancelled}")
      println(s"[main] queue size (before purge) = ${pool.getQueue.size()} (cancelled entries may remain)")
      pool.purge()
      println(s"[main] queue size (after purge) = ${pool.getQueue.size()} (cancelled entries removed)")

      // Interrupt the running task
      val interrupted = f1.cancel(true)
      println(s"[main] attempted to cancel running future f1 with cancel(true) -> returned $interrupted")

      // f1.get() will return the string the task returned (or Interrupted result)
      try {
        val r1 = f1.get()
        println(s"[main] f1.get() returned: $r1; f1.isDone=${f1.isDone}, f1.isCancelled=${f1.isCancelled}")
      } catch {
        case e: CancellationException => println(s"[main] f1 was cancelled: $e")
        case e: Exception => println(s"[main] f1.get() threw: $e")
      }
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 1: execute() vs submit() — different ways to submit tasks
   *
   * execute(Runnable):
   *   - Fire-and-forget: no Future returned
   *   - Cannot observe completion or result
   *   - Use when you only care about side effects, not completion
   *
   * submit(Runnable):
   *   - Returns Future[Unit]
   *   - Future.get() blocks until the task completes, then returns null
   *   - Useful for observing completion and catching exceptions via ExecutionException
   *
   * submit(Runnable, result):
   *   - Returns Future[T] where T is the preset result value
   *   - Task completes, Future.get() returns the preset value (not computed from the task)
   *   - Useful when you want a result handle but the actual result is determined before submission
   *
   * submit(Callable[A]):
   *   - Returns Future[A]
   *   - Task computes a value of type A
   *   - Future.get() blocks until task completes, then returns the computed value
   *   - If the task throws an exception, Future.get() rethrows it wrapped in ExecutionException
   */
  private def demoExecuteAndSubmit(): Unit = {
    val pool = Executors.newFixedThreadPool(2, namedFactory("submit-demo"))
    try {
      val executed = new CountDownLatch(1)

      pool.execute(new Runnable {
        override def run(): Unit = {
          println(s"  execute(Runnable): ran on ${Thread.currentThread().getName}")
          executed.countDown()
        }
      })
      executed.await()

      val runnableFuture: Future[_] = pool.submit(new Runnable {
        override def run(): Unit =
          println(s"  submit(Runnable): side effect on ${Thread.currentThread().getName}")
      })
      println(s"  submit(Runnable).get(): ${runnableFuture.get()}  (null because Runnable returns Unit)")

      val presetResult: Future[String] = pool.submit(new Runnable {
        override def run(): Unit =
          println(s"  submit(Runnable, result): work finished on ${Thread.currentThread().getName}")
      }, "DONE")
      println(s"  submit(Runnable, result).get(): ${presetResult.get()}")

      val callableResult: Future[Int] = pool.submit(new Callable[Int] {
        override def call(): Int = 21 * 2
      })
      println(s"  submit(Callable[Int]).get(): ${callableResult.get()}")
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 2: invokeAll() and invokeAny() — batch submission with different completion semantics
   *
   * invokeAll(tasks):
   *   - Submits ALL tasks and waits for EVERY one to complete
   *   - Returns a List of Futures (all completed or cancelled)
   *   - Useful: map/reduce patterns where you need all results before proceeding
   *
   * invokeAll(tasks, timeout, unit):
   *   - Like invokeAll, but with a timeout
   *   - After timeout, any unfinished tasks are cancelled (Future.isCancelled returns true)
   *   - Returns Futures where some may be cancelled
   *   - Useful: time-bound batch processing with fallback behavior for slow tasks
   *
   * invokeAny(tasks):
   *   - Submits ALL tasks, but returns after FIRST one completes successfully
   *   - Returns the computed result directly (not a Future)
   *   - Other tasks may still be running; often you should cancel them manually
   *   - Throws ExecutionException if a task fails, or NoSuchElementException if all fail
   *   - Useful: "race" multiple strategies and use the first successful result
   *
   * invokeAny(tasks, timeout, unit):
   *   - Like invokeAny, but if no task finishes within the timeout, throws TimeoutException
   *   - Useful: time-bound racing with a hard deadline
   */
  private def demoInvokeFunctions(): Unit = {
    val pool = Executors.newFixedThreadPool(3, namedFactory("invoke-demo"))
    try {
      val allTasks: java.util.List[Callable[String]] = List(
        callable("all-A", 80),
        callable("all-B", 120),
        callable("all-C", 40)
      ).asJava

      val allResults = pool.invokeAll(allTasks).asScala.map(_.get()).mkString(", ")
      println(s"  invokeAll(tasks): waits for every task -> $allResults")

      val timedTasks: java.util.List[Callable[String]] = List(
        callable("timed-fast", 50),
        callable("timed-slow", 500)
      ).asJava

      val timedResults = pool.invokeAll(timedTasks, 150, TimeUnit.MILLISECONDS).asScala
      timedResults.zipWithIndex.foreach { case (future, index) =>
        if (future.isCancelled)
          println(s"  invokeAll(timeout): task ${index + 1} was cancelled because timeout expired")
        else
          println(s"  invokeAll(timeout): task ${index + 1} result = ${future.get()}")
      }

      val anyResult = pool.invokeAny(List(
        callable("any-slow", 300),
        callable("any-fast", 60)
      ).asJava)
      println(s"  invokeAny(tasks): returns first successful result -> $anyResult")

      try {
        pool.invokeAny(List(callable("too-slow", 500)).asJava, 100, TimeUnit.MILLISECONDS)
        println("  invokeAny(timeout): unexpectedly completed")
      } catch {
        case _: TimeoutException =>
          println("  invokeAny(timeout): timed out before any task completed")
      }
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 3: Pool sizing and monitoring — tuning the pool and observing its behavior
   *
   * getCorePoolSize() / setCorePoolSize(n):
   *   - Core threads are kept alive even if idle
   *   - When tasks arrive, new threads are created up to corePoolSize
   *   - Example: corePoolSize=1 means at least 1 thread is always ready
   *
   * getMaximumPoolSize() / setMaximumPoolSize(n):
   *   - Maximum total threads the pool can create
   *   - When the queue is full, new threads are created up to maximumPoolSize
   *   - Beyond that, further submissions are rejected (AbortPolicy, etc.)
   *
   * Monitoring methods (all approximate; for performance reasons they don't acquire locks):
   *   - getPoolSize()          // Current number of threads (includes idle core threads)
   *   - getActiveCount()       // Threads currently executing a task (rough estimate)
   *   - getLargestPoolSize()   // Peak number of threads that have ever existed
   *   - getTaskCount()         // Approximate total tasks submitted to this pool lifetime
   *   - getCompletedTaskCount() // Approximate total tasks that have completed
   *   - getQueue().size()      // Queued tasks waiting for a thread (not yet running)
   *   - toString()             // Human-readable snapshot, e.g. ThreadPoolExecutor@hash[core=2, max=4, ...]
   *
   * Use case: Monitor pool health during stress tests, scale dynamically based on queue depth
   */
  private def demoSizingAndMonitoring(): Unit = {
    val pool = new ThreadPoolExecutor(
      1,
      3,
      1L,
      TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](2),
      namedFactory("sizing-demo"),
      new ThreadPoolExecutor.AbortPolicy()
    )

    try {
      println(s"  initial core size: ${pool.getCorePoolSize}")
      println(s"  initial max size : ${pool.getMaximumPoolSize}")

      pool.setMaximumPoolSize(4)
      pool.setCorePoolSize(2)
      println(s"  after setters    : core=${pool.getCorePoolSize}, max=${pool.getMaximumPoolSize}")

      val release = new CountDownLatch(1)
      (1 to 5).foreach { id =>
        pool.execute(new Runnable {
          override def run(): Unit = {
            try {
              println(s"  task-$id started on ${Thread.currentThread().getName}")
              release.await()
            } catch {
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
            }
          }
        })
      }

      Thread.sleep(150)
      println(s"  getPoolSize()           = ${pool.getPoolSize}")
      println(s"  getActiveCount()        = ${pool.getActiveCount}")
      println(s"  getLargestPoolSize()    = ${pool.getLargestPoolSize}")
      println(s"  getTaskCount()          = ${pool.getTaskCount}")
      println(s"  getCompletedTaskCount() = ${pool.getCompletedTaskCount}")
      println(s"  getQueue().size()       = ${pool.getQueue.size()}")
      println(s"  toString()              = $pool")

      release.countDown()
      pool.shutdown()
      pool.awaitTermination(2, TimeUnit.SECONDS)
      println(s"  completed after release = ${pool.getCompletedTaskCount}")
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 4: Prestart, thread factory, and keep-alive — thread lifecycle customization
   *
   * prestartCoreThread() / prestartAllCoreThreads():
   *   - By default, core threads are created only when tasks arrive
   *   - prestartCoreThread() creates ONE core thread eagerly (even if idle)
   *   - prestartAllCoreThreads() creates ALL core threads immediately
   *   - Useful: minimize latency on the first task by having threads ready
   *   - Also useful: ensure pool reaches steady state during startup
   *   - Returns number of threads newly created
   *
   * getThreadFactory() / setThreadFactory(...):
   *   - ThreadFactory is responsible for creating every new thread
   *   - By default, pool creates non-daemon, unnamed threads
   *   - Custom factory can set names (e.g., "pool-1-thread-1"), make threads daemon, etc.
   *   - Called by the pool whenever a new thread needs to be created
   *   - MUST call setThreadFactory() BEFORE submitting tasks to the pool
   *
   * getKeepAliveTime(unit) / setKeepAliveTime(time, unit):
   *   - Non-core threads exit after being idle for this duration
   *   - Core threads do NOT exit by default (they stay alive forever)
   *   - Example: if keepAliveTime=1min and a non-core thread is idle for 1min, it terminates
   *   - Use case: scale down the pool when demand decreases (cost savings)
   *
   * allowCoreThreadTimeOut(true):
   *   - Allows CORE threads to exit after keepAliveTime, just like non-core threads
   *   - By default (false), core threads never exit
   *   - Use case: make the pool completely idle when no work arrives
   */
  private def demoPrestartFactoryAndKeepAlive(): Unit = {
    val pool = new ThreadPoolExecutor(
      2,
      2,
      1L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable](),
      namedFactory("prestart-original")
    )

    try {
      println(s"  getPoolSize() before prestart = ${pool.getPoolSize}")
      println(s"  prestartCoreThread()          = ${pool.prestartCoreThread()}")
      println(s"  prestartAllCoreThreads()      = ${pool.prestartAllCoreThreads()}")
      println(s"  getPoolSize() after prestart  = ${pool.getPoolSize}")

      println(s"  getThreadFactory()            = ${pool.getThreadFactory}")
      pool.setThreadFactory(namedFactory("prestart-replacement"))
      println(s"  setThreadFactory(...); now    = ${pool.getThreadFactory}")

      println(s"  getKeepAliveTime(ms) before   = ${pool.getKeepAliveTime(TimeUnit.MILLISECONDS)}")
      pool.setKeepAliveTime(250, TimeUnit.MILLISECONDS)
      println(s"  getKeepAliveTime(ms) after    = ${pool.getKeepAliveTime(TimeUnit.MILLISECONDS)}")

      println(s"  allowsCoreThreadTimeOut()     = ${pool.allowsCoreThreadTimeOut()}")
      pool.allowCoreThreadTimeOut(true)
      println(s"  after allowCoreThreadTimeOut  = ${pool.allowsCoreThreadTimeOut()}")
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 5: remove() and purge() — task cancellation and queue cleanup
   *
   * remove(Runnable):
   *   - Attempts to remove a queued (not yet running) task from the queue
   *   - Returns true if the task was in the queue and successfully removed
   *   - Returns false if the task was not found (already running or not present)
   *   - The removed task will never execute
   *   - Use case: cancel a task you submitted but haven't started yet
   *
   * cancel(false) on a Future:
   *   - "mayInterruptIfRunning = false": only cancel if the task hasn't started
   *   - Returns true if successfully cancelled (task was queued)
   *   - Returns false if the task is already running or already completed
   *   - If cancelled, Future.get() throws CancellationException
   *   - NOTE: the task may remain in the queue as a cancelled entry
   *
   * cancel(true) on a Future:
   *   - "mayInterruptIfRunning = true": forcefully interrupt if running
   *   - Returns true if cancelled or interrupted
   *   - Returns false if already completed (no interruption possible)
   *   - If running, the executing thread receives an InterruptedException
   *   - More aggressive than cancel(false)
   *
   * purge():
   *   - Scans the internal queue and removes ALL cancelled Futures
   *   - Called after cancelling multiple tasks to reclaim queue space
   *   - Does nothing for regular Runnables (not wrapped in Future)
   *   - Return value: number of tasks actually purged (varies)
   *   - Use case: after bulk cancellation, clean up the queue before submitting new work
   *
   * Workflow example:
   *   1. Submit 10 tasks; 8 are queued (2 running)
   *   2. Call cancel(false) on 6 Futures -> 6 marked as cancelled in queue
   *   3. Call purge() -> the 6 cancelled entries are removed from queue
   *   4. Now queue size is 2 instead of 8 (more room for new tasks)
   */
  private def demoRemoveAndPurge(): Unit = {
    val pool = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      namedFactory("queue-demo")
    )

    val releaseFirstTask = new CountDownLatch(1)

    try {
      pool.execute(new Runnable {
        override def run(): Unit = {
          try {
            releaseFirstTask.await()
            println(s"[${Thread.currentThread().getName}] first task completed")
          }

          catch {
            case _: InterruptedException => Thread.currentThread().interrupt()
          }
        }
      })

      val removable = new Runnable {
        override def run(): Unit =
          println(s"[${Thread.currentThread().getName}]  remove target should not run")
      }

      pool.execute(removable)
      println(s"  queue size before remove = ${pool.getQueue.size()}")
      println(s"  remove(runnable)         = ${pool.remove(removable)}")
      println(s"  queue size after remove  = ${pool.getQueue.size()}")


      pool.execute(removable)
      println(s"  Queue size after adding removable = ${pool.getQueue.size()}")


      val queuedFuture1 = pool.submit(new Callable[String] {
        override def call(): String = "cancelled-1"
      })
      val queuedFuture2 = pool.submit(new Callable[String] {
        override def call(): String = "cancelled-2"
      })

      println(s"  queue size before cancel = ${pool.getQueue.size()}")
      queuedFuture1.cancel(false)
      queuedFuture2.cancel(false)
      println(s"  queue size after cancel  = ${pool.getQueue.size()}  (cancelled tasks can remain queued)")
      pool.purge()
      println(s"  queue size after purge   = ${pool.getQueue.size()}  (purge removes cancelled queued futures)")
    } finally {
      releaseFirstTask.countDown()
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 6: Rejection handler — custom handling when the pool is at capacity
   *
   * When do rejections occur?
   *   - execute(Runnable) is called when:
   *     1. Pool size >= maximumPoolSize AND
   *     2. Any task queue is full (for bounded queues)
   *   - The pool defines a policy for what happens next (default: AbortPolicy, throw exception)
   *
   * getRejectedExecutionHandler():
   *   - Returns the current RejectedExecutionHandler policy
   *   - Built-in policies (ThreadPoolExecutor static inner classes):
   *     * AbortPolicy (default)      - throw RejectedExecutionException
   *     * CallerRunsPolicy           - run the task in the calling thread (blocks caller)
   *     * DiscardPolicy              - silently drop the task
   *     * DiscardOldestPolicy        - remove oldest queued task, retry submission
   *
   * setRejectedExecutionHandler(handler):
   *   - Install a custom RejectedExecutionHandler
   *   - Interface: rejectedExecution(Runnable, ThreadPoolExecutor)
   *   - Called when submission is rejected; can log, drop, rethrow, or custom action
   *   - MUST call before submitting tasks to take effect on rejections
   *
   * Custom handler use cases:
   *   - Logging: record which tasks were rejected and when
   *   - Metrics: increment a counter, alerting on too many rejections
   *   - Fallback: run in a separate pool, write to a queue, etc.
   *
   * Tuning to avoid rejection:
   *   - Increase maximumPoolSize to handle bursts
   *   - Use an unbounded queue (LinkedBlockingQueue with no maxSize)
   *   - Use CallerRunsPolicy as a circuit breaker (blocks caller if pool overloaded)
   */
  private def demoRejectionHandler(): Unit = {
    val rejected = new AtomicInteger(0)
    val release = new CountDownLatch(1)
    val pool = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue[Runnable](1),
      namedFactory("reject-demo"),
      new ThreadPoolExecutor.AbortPolicy()
    )

    try {
      println(s"  default getRejectedExecutionHandler() = ${pool.getRejectedExecutionHandler}")

      pool.setRejectedExecutionHandler(new RejectedExecutionHandler {
        override def rejectedExecution(runnable: Runnable, executor: ThreadPoolExecutor): Unit = {
          rejected.incrementAndGet()
          println(s"  custom rejectedExecution(): rejected task while queue size=${executor.getQueue.size()}")
        }
      })
      println(s"  after setRejectedExecutionHandler()   = ${pool.getRejectedExecutionHandler}")

      pool.execute(blockingTask("running", release))
      pool.execute(blockingTask("queued", release))
      pool.execute(new Runnable {
        override def run(): Unit =
          println("  this task is rejected and will not run")
      })

      println(s"  rejected count = ${rejected.get()}")
    } finally {
      release.countDown()
      shutdownCleanly(pool)
    }
  }

  /**
   * DEMO 7: Shutdown lifecycle — graceful vs abrupt pool termination
   *
   * GRACEFUL SHUTDOWN (shutdown + awaitTermination):
   *   ┌─────────────────────────────────────────────────────────┐
   *   │ RUNNING ──shutdown()──> SHUTDOWN ──[tasks end]──> TERMINATED
   *   │           (no new     (no new tasks   (all existing
   *   │            tasks)     accepted, wait   tasks done)
   *   │                       for existing)
   *   └─────────────────────────────────────────────────────────┘
   *
   *   shutdown():
   *     - Transitions pool to SHUTDOWN state
   *     - Future submit() calls throw RejectedExecutionException
   *     - Queued and running tasks continue normally
   *     - Non-blocking; returns immediately
   *
   *   awaitTermination(timeout, unit):
   *     - Blocks until all tasks complete OR timeout expires
   *     - Returns true if terminated cleanly, false if timed out
   *     - After this, you can check isTerminated()
   *     - Use case: wait a reasonable time, then use shutdownNow() for stragglers
   *
   *   isTerminating():
   *     - True after shutdown() is called but before all tasks complete
   *     - Useful: detect the pool is in transition
   *
   * ABRUPT SHUTDOWN (shutdownNow):
   *   ┌──────────────────────────────────────────────────┐
   *   │ RUNNING ──shutdownNow()──> SHUTDOWN ──> TERMINATED
   *   │          (cancel queued,    (current tasks
   *   │           interrupt running) may still be running)
   *   └──────────────────────────────────────────────────┘
   *
   *   shutdownNow():
   *     - Transitions pool to SHUTDOWN state immediately
   *     - Interrupts all currently running threads (Thread.interrupt())
   *     - Returns List of Runnables that were in the queue (never started)
   *     - Non-blocking; you may need to awaitTermination() after
   *     - Use case: emergency stop (e.g., JVM shutdown hook)
   *
   * Lifecycle state machine:
   *   - RUNNING: normal operation
   *   - SHUTDOWN: initiated via shutdown(), no new tasks accepted
   *   - TERMINATED: all threads have exited, no more tasks will run
   *
   * Best practices:
   *   1. Call shutdown() first (graceful)
   *   2. Call awaitTermination(timeout) and wait
   *   3. If times out, call shutdownNow() for stragglers
   *   4. Call awaitTermination(timeout) again to be sure
   *   5. Only after that is the pool truly terminated
   *
   * This pattern is implemented in the shutdownCleanly() helper function below.
   */
  private def demoShutdownLifecycle(): Unit = {
    val gracefulPool = Executors.newFixedThreadPool(1, namedFactory("shutdown-graceful"))
    gracefulPool.submit(new Runnable {
      override def run(): Unit = Thread.sleep(100)
    })

    println(s"  before shutdown: isShutdown=${gracefulPool.isShutdown}, isTerminated=${gracefulPool.isTerminated}")
    gracefulPool.shutdown()
    println(s"  after shutdown : isShutdown=${gracefulPool.isShutdown}, isTerminated=${gracefulPool.isTerminated}")
    println(s"  awaitTermination(...) = ${gracefulPool.awaitTermination(1, TimeUnit.SECONDS)}")
    println(s"  after await    : isShutdown=${gracefulPool.isShutdown}, isTerminated=${gracefulPool.isTerminated}")

    val abruptPool = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      namedFactory("shutdown-abrupt")
    )

    abruptPool.execute(blockingTask("abrupt-running", new CountDownLatch(1)))
    abruptPool.execute(new Runnable {
      override def run(): Unit =
        println("  abrupt queued task should be returned by shutdownNow")
    })

    Thread.sleep(50)
    println(s"  before shutdownNow: isTerminating=${abruptPool.isTerminating}, queue=${abruptPool.getQueue.size()}")
    val notStarted = abruptPool.shutdownNow()
    println(s"  shutdownNow() returned ${notStarted.size()} queued task(s)")
    println(s"  after shutdownNow : isShutdown=${abruptPool.isShutdown}, isTerminating=${abruptPool.isTerminating}")
    abruptPool.awaitTermination(1, TimeUnit.SECONDS)
    println(s"  after termination : isTerminated=${abruptPool.isTerminated}")
  }

  /**
   * Helper: Creates a simple Callable[String] that sleeps then returns a label.
   * Used in demos to simulate long-running tasks with predictable delays.
   */
  private def callable(name: String, sleepMillis: Long): Callable[String] = new Callable[String] {
    override def call(): String = {
      Thread.sleep(sleepMillis)
      s"$name on ${Thread.currentThread().getName}"
    }
  }

  /**
   * Helper: Creates a Runnable that blocks on a CountDownLatch until released.
   * Used in demos to keep tasks alive while main thread checks pool state,
   * without the tasks completing prematurely.
   *
   * Properly handles InterruptedException and restores the interrupt flag
   * (standard pattern for cancellable blocking code).
   */
  private def blockingTask(label: String, release: CountDownLatch): Runnable = new Runnable {
    override def run(): Unit = {
      try {
        println(s"  $label started on ${Thread.currentThread().getName}")
        release.await()
      } catch {
        case _: InterruptedException =>
          println(s"  $label interrupted")
          Thread.currentThread().interrupt()
      }
    }
  }

  /**
   * Helper: Creates a ThreadFactory with customized thread names.
   * Each factory maintains its own AtomicInteger counter to assign unique IDs to threads.
   *
   * Why use a custom ThreadFactory?
   *   - Default factory creates threads with generic names like "pool-1-thread-1"
   *   - Custom names make logs and thread dumps much more readable
   *   - Example output in logs: "[submit-demo-1] execute(Runnable): ran on..."
   *   - Also useful for: making threads daemon, setting custom priority, etc.
   */
  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextId.getAndIncrement()}")
  }

  /**
   * Helper: Clean shutdown pattern — the recommended way to shut down a pool.
   *
   * Why not just call shutdown()?
   *   - If a task hangs or never completes, shutdown() waits forever
   *   - This leaves thread resources dangling
   *
   * The three-step shutdown pattern (used here):
   *   1. pool.shutdown() — graceful: stop accepting new tasks, wait for existing ones
   *   2. pool.awaitTermination(timeout) — wait up to this long for completion
   *   3. If timeout: pool.shutdownNow() — forceful: interrupt running tasks and cancel queued ones
   *   4. Second awaitTermination(timeout) — verify all tasks are finally gone
   *
   * Also note: InterruptedException handling with re-raise
   *   - If awaitTermination() is itself interrupted, we:
   *     (a) Call shutdownNow() to clean up
   *     (b) Call Thread.currentThread().interrupt() to restore the flag
   *   - This allows higher-level code to still see the interruption request
   *
   * Use this in your own code whenever you need to clean up a pool.
   */
  private def shutdownCleanly(pool: ExecutorService): Unit = {
    pool.shutdown()
    try {
      if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
        pool.shutdownNow()
        pool.awaitTermination(2, TimeUnit.SECONDS)
      }
    } catch {
      case _: InterruptedException =>
        pool.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  /**
   * Helper: Prints a visually clear section header to separate demos in output.
   */
  private def section(number: Int, title: String): Unit = {
    println()
    println("=" * 78)
    println(s"[$number] $title")
    println("=" * 78)
  }
}
