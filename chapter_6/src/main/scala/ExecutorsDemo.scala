/*
 * Chapter 6 -- Executor examples
 *
 * This file demonstrates three executor strategies with the same workload:
 *
 *   TaskPerThreadExecutor
 *       Creates one new JVM thread for every submitted task.
 *
 *   TaskPoolExecutor
 *       Reuses a fixed-size pool of worker threads. Extra tasks wait in the
 *       executor queue until a worker becomes free.
 *
 *   SingleThreadExecutor
 *       Uses exactly one worker thread. Tasks run one at a time, in submission
 *       order.
 *
 * The important design point is that the task-submission code only depends on
 * java.util.concurrent.Executor. To change the execution policy, change the
 * executor factory, not the task code.
 *
 * Run all demos:
 *   sbt "chapter6/run"
 *
 * Run one executor only:
 *   sbt "chapter6/run per-task"
 *   sbt "chapter6/run pool"
 *   sbt "chapter6/run single"
 */

import java.util.concurrent.{CountDownLatch, Executor, ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

trait ManagedExecutor extends Executor {
  def label: String
  def shutdown(): Unit
}

final class TaskPerThreadExecutor(threadPrefix: String = "task-per-thread") extends ManagedExecutor {
  private val nextThreadId = new AtomicInteger(1)

  override val label: String = "TaskPerThreadExecutor"

  override def execute(command: Runnable): Unit = {
    val worker = new Thread(command, s"$threadPrefix-${nextThreadId.getAndIncrement()}")
    worker.start()
  }

  override def shutdown(): Unit = {
    // No backing ExecutorService exists here. Each submitted task owns its thread.
  }
}

final class TaskPoolExecutor(poolSize: Int = 3, threadPrefix: String = "task-pool") extends ManagedExecutor {
  private val service = Executors.newFixedThreadPool(poolSize, ExecutorTools.namedThreadFactory(threadPrefix))

  override val label: String = s"TaskPoolExecutor(poolSize=$poolSize)"

  override def execute(command: Runnable): Unit =
    service.execute(command)

  override def shutdown(): Unit =
    ExecutorTools.shutdownGracefully(service)
}

final class SingleThreadExecutor(threadPrefix: String = "single-thread") extends ManagedExecutor {
  private val service = Executors.newSingleThreadExecutor(ExecutorTools.namedThreadFactory(threadPrefix))

  override val label: String = "SingleThreadExecutor"

  override def execute(command: Runnable): Unit =
    service.execute(command)

  override def shutdown(): Unit =
    ExecutorTools.shutdownGracefully(service)
}

private object ExecutorTools {
  def namedThreadFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextThreadId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextThreadId.getAndIncrement()}")
  }

  def shutdownGracefully(service: ExecutorService): Unit = {
    service.shutdown()
    try {
      if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
        service.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        service.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }
}

object ExecutorDemo {
  private val TaskCount = 6
  private val WorkMillis = 300L

  def main(args: Array[String]): Unit = {
    val executors =
      if (args.isEmpty) allExecutors()
      else args.map(executorFromName).toList

    executors.foreach { executor =>
      try runSameWorkload(executor.label, executor)
      finally executor.shutdown()
    }
  }

  private def allExecutors(): List[ManagedExecutor] =
    List(
      new TaskPerThreadExecutor(),
      new TaskPoolExecutor(poolSize = 3),
      new SingleThreadExecutor()
    )

  private def executorFromName(name: String): ManagedExecutor =
    name.toLowerCase match {
      case "per-task" | "task-per-thread" | "thread-per-task" =>
        new TaskPerThreadExecutor()
      case "pool" | "task-pool" =>
        new TaskPoolExecutor(poolSize = 3)
      case "single" | "single-thread" =>
        new SingleThreadExecutor()
      case other =>
        throw new IllegalArgumentException(
          s"Unknown executor '$other'. Use per-task, pool, or single."
        )
    }

  private def runSameWorkload(label: String, executor: Executor): Unit = {
    println()
    println(s"=== $label ===")
    println(s"Submitting $TaskCount tasks through the same Executor interface.")

    val finished = new CountDownLatch(TaskCount)
    val startedAtNanos = System.nanoTime()

    (1 to TaskCount).foreach { taskId =>
      executor.execute(new Runnable {
        override def run(): Unit = {
          val threadName = Thread.currentThread().getName
          println(f"task $taskId%02d started  on $threadName")

          try {
            Thread.sleep(WorkMillis)
            println(f"task $taskId%02d finished on $threadName")
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              println(f"task $taskId%02d interrupted on $threadName")
          } finally {
            finished.countDown()
          }
        }
      })
    }

    finished.await()
    val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos)
    println(s"$label completed all tasks in about ${elapsedMillis}ms")
  }
}
