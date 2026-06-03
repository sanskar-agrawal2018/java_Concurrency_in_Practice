/*
 * Executor Lifecycle Demo
 *
 * Demonstrates the practical lifecycle of a Java/Scala ExecutorService:
 *
 *   1. CREATED
 *      The ThreadPoolExecutor object exists, but worker threads are created
 *      lazily when tasks are submitted.
 *
 *   2. RUNNING
 *      The executor accepts new tasks. Some tasks run immediately on worker
 *      threads while extra tasks wait in the work queue.
 *
 *   3. SHUTDOWN
 *      shutdown() stops accepting new tasks, but already-submitted tasks keep
 *      running. Queued tasks are still executed.
 *
 *   4. STOPPING
 *      shutdownNow() attempts to interrupt running tasks and returns tasks
 *      that were still waiting in the queue.
 *
 *   5. TERMINATED
 *      All worker threads have stopped and the executor is fully finished.
 *
 * Run:
 *   sbt "chapter6/runMain ExecutorLifecycleDemo"
 *
 * Backward-compatible run command for this file:
 *   sbt "chapter6/runMain BlockingQueueFullDemo"
 */

import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

object ExecutorLifecycleDemo {
  def main(args: Array[String]): Unit = {
    demoGracefulShutdownLifecycle()
    println()
    println("=" * 80)
    println()
    demoImmediateShutdownLifecycle()
  }

  private def demoGracefulShutdownLifecycle(): Unit = {
    println("DEMO 1: graceful lifecycle with shutdown()")
    println("=" * 80)

    val releaseTasks = new CountDownLatch(1)
    val firstTwoTasksStarted = new CountDownLatch(2)
    val executor = lifecycleExecutor(name = "graceful-pool", workers = 2, queueCapacity = 2)

    printState("after creation", executor)

    (1 to 4).foreach { taskId =>
      executor.execute(blockingTask(taskId, firstTwoTasksStarted, releaseTasks))
      printState(s"after submitting task $taskId", executor)
    }

    firstTwoTasksStarted.await()
    println()
    println("Two tasks are running and two tasks are queued.")
    printState("running", executor)

    println()
    println("Calling shutdown(): executor will reject new tasks but finish existing work.")
    executor.shutdown()
    printState("after shutdown()", executor)

    try {
      executor.execute(loggingTask(99))
    } catch {
      case _: RejectedExecutionException =>
        println("late task rejected because shutdown() was already called")
    }

    println()
    println("Releasing running tasks. Queued tasks should still execute after shutdown().")
    releaseTasks.countDown()

    val terminated = executor.awaitTermination(10, TimeUnit.SECONDS)
    println(s"awaitTermination returned: $terminated")
    printState("after graceful termination", executor)
  }

  private def demoImmediateShutdownLifecycle(): Unit = {
    println("DEMO 2: immediate lifecycle with shutdownNow()")
    println("=" * 80)

    val releaseTasks = new CountDownLatch(1)
    val firstTwoTasksStarted = new CountDownLatch(2)
    val executor = lifecycleExecutor(name = "stop-now-pool", workers = 2, queueCapacity = 4)

    printState("after creation", executor)

    (1 to 6).foreach { taskId =>
      executor.execute(blockingTask(taskId, firstTwoTasksStarted, releaseTasks))
      printState(s"after submitting task $taskId", executor)
    }

    firstTwoTasksStarted.await()
    println()
    println("Two tasks are running and four tasks are queued.")
    printState("before shutdownNow()", executor)

    println()
    println("Calling shutdownNow(): running tasks are interrupted; queued tasks are returned.")
    val neverStarted = executor.shutdownNow()
    printState("after shutdownNow()", executor)
    println(s"shutdownNow() returned ${neverStarted.size()} queued task(s) that never started")

    releaseTasks.countDown()

    val terminated = executor.awaitTermination(10, TimeUnit.SECONDS)
    println(s"awaitTermination returned: $terminated")
    printState("after immediate termination", executor)
  }

  private def lifecycleExecutor(
      name: String,
      workers: Int,
      queueCapacity: Int
  ): LifecycleLoggingExecutor = {
    new LifecycleLoggingExecutor(
      workers,
      workers,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](queueCapacity),
      namedThreadFactory(name)
    )
  }

  private def blockingTask(
      taskId: Int,
      startedSignal: CountDownLatch,
      releaseSignal: CountDownLatch
  ): Runnable = new Runnable {
    override def run(): Unit = {
      val threadName = Thread.currentThread().getName
      println(s"task-$taskId started on $threadName")
      startedSignal.countDown()

      try {
        releaseSignal.await()
        Thread.sleep(200)
        println(s"task-$taskId finished on $threadName")
      } catch {
        case _: InterruptedException =>
          println(s"task-$taskId interrupted on $threadName")
          Thread.sleep(300)
          println(s"task-$taskId cleanup finished on $threadName")
          Thread.currentThread().interrupt()
      }
    }

    override def toString: String = s"task-$taskId"
  }

  private def loggingTask(taskId: Int): Runnable = new Runnable {
    override def run(): Unit =
      println(s"task-$taskId executed on ${Thread.currentThread().getName}")

    override def toString: String = s"task-$taskId"
  }

  private def printState(label: String, executor: ThreadPoolExecutor): Unit = {
    println(
      f"[$label%-30s] " +
        f"poolSize=${executor.getPoolSize}%d, " +
        f"active=${executor.getActiveCount}%d, " +
        f"queued=${executor.getQueue.size()}%d, " +
        f"completed=${executor.getCompletedTaskCount}%d, " +
        f"isShutdown=${executor.isShutdown}%s, " +
        f"isTerminating=${executor.isTerminating}%s, " +
        f"isTerminated=${executor.isTerminated}%s"
    )
  }

  private def namedThreadFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextId.getAndIncrement()}")
  }
}

final class LifecycleLoggingExecutor(
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveTime: Long,
    unit: TimeUnit,
    workQueue: LinkedBlockingQueue[Runnable],
    threadFactory: ThreadFactory
) extends ThreadPoolExecutor(
      corePoolSize,
      maximumPoolSize,
      keepAliveTime,
      unit,
      workQueue,
      threadFactory
    ) {

  override protected def beforeExecute(thread: Thread, runnable: Runnable): Unit = {
    super.beforeExecute(thread, runnable)
    println(s"beforeExecute: $runnable is about to run on ${thread.getName}")
  }

  override protected def afterExecute(runnable: Runnable, error: Throwable): Unit = {
    super.afterExecute(runnable, error)

    val failure =
      if (error == null) ""
      else s", failed with ${error.getClass.getSimpleName}: ${error.getMessage}"

    println(s"afterExecute: $runnable completed$failure")
  }

  override protected def terminated(): Unit = {
    println("terminated hook: executor has reached the TERMINATED state")
    super.terminated()
  }
}

object BlockingQueueFullDemo {
  def main(args: Array[String]): Unit =
    ExecutorLifecycleDemo.main(args)
}
