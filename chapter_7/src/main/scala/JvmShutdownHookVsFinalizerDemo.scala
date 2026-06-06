/**
 * Chapter 7 -- JVM shutdown, application threads, shutdown hooks, and finalizers
 *
 * A JVM does not begin normal shutdown merely because main() returns. It waits
 * until every non-daemon application thread has completed. Shutdown hooks then
 * run as part of JVM shutdown.
 * critical cleanup
 *
 * Run each scenario in a separate JVM:
 *   sbt "chapter7/runMain JvmShutdownHookVsFinalizerDemo main-finishes-first"
 *   sbt "chapter7/runMain JvmShutdownHookVsFinalizerDemo worker-finishes-first"
 */

import java.util.concurrent.{CountDownLatch, TimeUnit}

object JvmShutdownHookVsFinalizerDemo {
  private val MainFinishesFirst = "main-finishes-first"
  private val WorkerFinishesFirst = "worker-finishes-first"

  def main(args: Array[String]): Unit = {

//    Run the first demo where main() returns while a non-daemon
//    worker is still running. The worker keeps the JVM alive,
//    allowing finalization and shutdown hooks to run before
//    the JVM exits.
//    println(s"===Demo 1 ${MainFinishesFirst} ===")
//    demoMainFinishesFirst()
//
//    println(s"\n===Demo 2 ${WorkerFinishesFirst} ===")
//    demoWorkerFinishesFirst()
    if (args.length != 1) {
      println(s"Usage: sbt \"chapter7/runMain JvmShutdownHookVsFinalizerDemo <demo-mode>\"")
      println(s"  where <demo-mode> is either '$MainFinishesFirst' or '$WorkerFinishesFirst'")
    } else {
      args(0) match {
        case MainFinishesFirst => demoMainFinishesFirst()
        case WorkerFinishesFirst => demoWorkerFinishesFirst()
        case other =>
          println(s"Unknown demo mode '$other'. Expected '$MainFinishesFirst' or '$WorkerFinishesFirst'.")
      }
    }


  }

  /**
   * main() returns first, but the non-daemon worker keeps the JVM alive.
   * The worker gives an unreachable object a chance to be finalized and then
   * completes. Only after the worker completes can normal JVM shutdown begin.
   */
  private def demoMainFinishesFirst(): Unit = {
    println("DEMO 1: main finishes first; non-daemon worker keeps JVM alive")
    registerShutdownHook("demo-1")

    val mainReturning = new CountDownLatch(1)
    val workerStarted = new CountDownLatch(1)

    val worker = new Thread(new Runnable {
      override def run(): Unit = {
        println("[demo-1][worker] started as a non-daemon application thread")
        workerStarted.countDown()
        mainReturning.await()

        // Give main() time to return before continuing the demonstration.
        Thread.sleep(200)
        println("[demo-1][worker] still running after main announced its return")

        println("[demo-1][worker] completed")
      }
    }, "demo-1-application-worker")

    worker.start()
    workerStarted.await()

    println("[demo-1][main] returning now while worker is still running")
    mainReturning.countDown()
    // Do not join: returning demonstrates that the worker keeps the JVM alive.
  }

  /**
   * The main thread waits for the non-daemon worker to finish. Once main()
   * returns, no application threads remain, so JVM shutdown and its hook begin.
   */
  private def demoWorkerFinishesFirst(): Unit = {
    println("DEMO 2: worker finishes first; main returns last")
    registerShutdownHook("demo-2")

    val worker = new Thread(new Runnable {
      override def run(): Unit = {
        println("[demo-2][worker] started")
        Thread.sleep(200)
        println("[demo-2][worker] completed")
      }
    }, "demo-2-application-worker")

    worker.start()
    worker.join()

    println("[demo-2][main] worker has completed")
    println("[demo-2][main] returning now")
  }

  private def registerShutdownHook(demo: String): Unit = {
    val hook = new Thread(new Runnable {
      override def run(): Unit =
        println(s"[$demo][shutdown-hook][${Thread.currentThread().getName}] started during JVM shutdown")
    }, s"$demo-shutdown-hook")

    Runtime.getRuntime.addShutdownHook(hook)
  }

}
