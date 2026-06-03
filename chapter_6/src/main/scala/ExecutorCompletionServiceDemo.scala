/*
 * ExecutorCompletionService Demo
 *
 * ExecutorCompletionService combines:
 *   - ExecutorService: runs submitted Callable tasks on worker threads
 *   - BlockingQueue[Future[T]]: stores finished tasks in completion order
 *
 * Why use it?
 *   If you submit many independent tasks, normal Future.get() in submission
 *   order can block behind a slow task. ExecutorCompletionService lets you
 *   consume whichever task finishes first.
 *
 * Run:
 *   sbt "chapter6/runMain ExecutorCompletionServiceDemo"
 */

import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

object ExecutorCompletionServiceDemo {
  def main(args: Array[String]): Unit = {
    demoCompletionOrder()
    println()
    println("=" * 85)
    println()
    demoFailureHandling()
    println()
    println("=" * 85)
    println()
    demoFirstSuccessfulResult()
  }

  private def demoCompletionOrder(): Unit = {
    println("DEMO 1: collect results in completion order")
    println("=" * 85)

    val executor = Executors.newFixedThreadPool(3, namedThreadFactory("completion-worker"))
    val completionService = new ExecutorCompletionService[WorkResult](executor)

    val tasks = List(
      Work("slow-report", 900),
      Work("fast-cache", 200),
      Work("medium-api", 500),
      Work("tiny-metadata", 100),
      Work("batch-export", 700)
    )

    try {
      tasks.foreach { work =>
        completionService.submit(workTask(work))
        println(s"submitted ${work.name} (${work.delayMillis}ms)")
      }

      println()
      println("Reading from completionService.take():")
      tasks.indices.foreach { _ =>
        val completedFuture = completionService.take()
        val result = completedFuture.get()
        println(
          s"received ${result.name} after ${result.delayMillis}ms " +
            s"from ${result.threadName}"
        )
      }
    } finally {
      shutdownGracefully(executor)
    }
  }

  private def demoFailureHandling(): Unit = {
    println("DEMO 2: handle task failures as completed futures arrive")
    println("=" * 85)

    val executor = Executors.newFixedThreadPool(3, namedThreadFactory("failure-worker"))
    val completionService = new ExecutorCompletionService[String](executor)

    val submittedCount = 4

    try {
      completionService.submit(successfulTask("load-users", 250))
      completionService.submit(failingTask("load-payments", 150))
      completionService.submit(successfulTask("load-products", 100))
      completionService.submit(failingTask("load-inventory", 300))

      (1 to submittedCount).foreach { _ =>
        val completedFuture = completionService.take()

        try {
          println(s"success: ${completedFuture.get()}")
        } catch {
          case e: ExecutionException =>
            val cause = e.getCause
            println(s"failure: ${cause.getClass.getSimpleName}: ${cause.getMessage}")
        }
      }
    } finally {
      shutdownGracefully(executor)
    }
  }

  private def demoFirstSuccessfulResult(): Unit = {
    println("DEMO 3: take the first successful result and cancel the rest")
    println("=" * 85)

    val executor = Executors.newFixedThreadPool(3, namedThreadFactory("replica-worker"))
    val completionService = new ExecutorCompletionService[String](executor)

    val futures = List(
      completionService.submit(failingTask("primary", 300)),
      completionService.submit(successfulTask("secondary", 500)),
      completionService.submit(successfulTask("backup", 900))
    )

    try {
      val answer = firstSuccessfulResult(completionService, attempts = futures.size)
      println(s"first successful result: $answer")
    } finally {
      futures.foreach(_.cancel(true))
      shutdownGracefully(executor)
    }
  }

  private def firstSuccessfulResult(
      completionService: ExecutorCompletionService[String],
      attempts: Int
  ): String = {
    var lastFailure: Throwable = null

    (1 to attempts).foreach { _ =>
      val completedFuture = completionService.take()

      try {
        return completedFuture.get()
      } catch {
        case e: ExecutionException =>
          lastFailure = e.getCause
          println(s"ignoring failed attempt: ${lastFailure.getMessage}")
      }
    }

    throw new RuntimeException("all attempts failed", lastFailure)
  }

  private def workTask(work: Work): Callable[WorkResult] = new Callable[WorkResult] {
    override def call(): WorkResult = {
      println(s"[${Thread.currentThread().getName}] starting ${work.name} (${work.delayMillis}ms)")
      sleep(work.delayMillis)
      WorkResult(
        name = work.name,
        delayMillis = work.delayMillis,
        threadName = Thread.currentThread().getName
      )
    }
  }

  private def successfulTask(name: String, delayMillis: Long): Callable[String] =
    new Callable[String] {
      override def call(): String = {
        sleep(delayMillis)
        s"$name completed on ${Thread.currentThread().getName}"
      }
    }

  private def failingTask(name: String, delayMillis: Long): Callable[String] =
    new Callable[String] {
      override def call(): String = {
        sleep(delayMillis)
        throw new IllegalStateException(s"$name failed")
      }
    }

  private def sleep(delayMillis: Long): Unit = {
    try {
      Thread.sleep(delayMillis)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("task interrupted")
    }
  }

  private def shutdownGracefully(executor: ExecutorService): Unit = {
    executor.shutdown()

    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        val pendingTasks = executor.shutdownNow()
        println(s"forced shutdown; ${pendingTasks.size()} task(s) never started")
      }
    } catch {
      case _: InterruptedException =>
        executor.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  private def namedThreadFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextThreadId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextThreadId.getAndIncrement()}")
  }
}

final case class Work(name: String, delayMillis: Long)

final case class WorkResult(name: String, delayMillis: Long, threadName: String)
