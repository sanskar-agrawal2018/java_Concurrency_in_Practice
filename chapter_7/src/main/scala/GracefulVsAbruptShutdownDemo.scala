/**
 * Chapter 7 -- ExecutorService shutdown modes
 *
 * ExecutorService offers two ways to shut down:
 *
 *   shutdown()     GRACEFUL -- no new tasks accepted; tasks already submitted
 *                  (both running and queued) are allowed to run to completion.
 *                  Does NOT interrupt any thread.
 *
 *   shutdownNow()  ABRUPT   -- no new tasks accepted; running threads are
 *                  interrupted; the queue is drained and the un-started tasks
 *                  are returned as List[Runnable] so the caller can decide what
 *                  to do with them (log, re-queue, discard, etc.).
 *
 * State transitions (observable via isShutdown / isTerminated):
 *
 *   [RUNNING] --shutdown()/shutdownNow()--> [SHUTDOWN/STOP]
 *             --all tasks finish----------> [TERMINATED]
 *
 *   isShutdown()   true after shutdown() OR shutdownNow() is called
 *   isTerminated() true only after every last thread has stopped
 *
 * Demos:
 *   1. Graceful  -- shutdown(): queued tasks finish, running tasks finish
 *   2. Abrupt    -- shutdownNow(): unstarted tasks returned, running interrupted
 *   3. States    -- watch isShutdown / isTerminated / awaitTermination live
 *   4. Rescue    -- re-submit the tasks returned by shutdownNow() to a new pool
 *
 * Run:
 *   sbt "chapter7/runMain GracefulVsAbruptShutdownDemo"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

object GracefulVsAbruptShutdownDemo {

  def main(args: Array[String]): Unit = {
    section(1, "shutdown()  -- GRACEFUL: every submitted task runs to completion")
    demoGraceful()

    section(2, "shutdownNow() -- ABRUPT: interrupt running, return un-started tasks")
    demoAbrupt()

    section(3, "State transitions: isShutdown / isTerminated / awaitTermination")
    demoStates()

    section(4, "Rescue: re-submit the returned tasks to a fresh pool")
    demoRescue()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 1. GRACEFUL -- shutdown()
  //    Pool of 2 threads, 5 tasks submitted.
  //    After shutdown():
  //      - The 2 running tasks finish normally.
  //      - The 3 queued tasks are picked up and also finish normally.
  //      - Any new submit AFTER shutdown() throws RejectedExecutionException.
  // ─────────────────────────────────────────────────────────────────────────
  private def demoGraceful(): Unit = {
    val pool      = Executors.newFixedThreadPool(2)
    val completed = new AtomicInteger(0)

    println("  [main] Submitting 5 tasks to a 2-thread pool ...")
    (1 to 5).foreach { i =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          println(s"  [Task-$i][${Thread.currentThread().getName}] started")
          Thread.sleep(300)
          completed.incrementAndGet()
          println(s"  [Task-$i] finished  (completed so far: ${completed.get})")
        }
      })
    }

    Thread.sleep(100)  // let tasks 1 & 2 start running

    println()
    println("  [main] calling shutdown() -- graceful, no new tasks accepted")
    pool.shutdown()
    println(s"  [main] isShutdown=${pool.isShutdown}  isTerminated=${pool.isTerminated}")

    // Attempt to submit AFTER shutdown -- always rejected
    try {
      pool.submit(new Runnable { def run(): Unit = println("  [late task] this should never print") })
      println("  [main] late submit succeeded (unexpected!)")
    } catch {
      case _: RejectedExecutionException =>
        println("  [main] late submit threw RejectedExecutionException (expected -- pool is shut down)")
    }

    // Wait for all tasks to drain
    val terminated = pool.awaitTermination(5, TimeUnit.SECONDS)
    println()
    println(s"  Results:")
    println(s"    Tasks completed  : ${completed.get()} / 5")
    println(s"    Terminated       : $terminated")
    println(s"    isTerminated     : ${pool.isTerminated}")
    println("  GRACEFUL: every task that was submitted ran to completion.")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 2. ABRUPT -- shutdownNow()
  //    Pool of 2 threads, 5 tasks submitted.
  //    After shutdownNow():
  //      - The 2 running tasks receive Thread.interrupt() -- they stop early.
  //      - The 3 queued tasks are DRAINED from the work queue and returned.
  //      - Returned list contains the Runnables exactly as submitted.
  // ─────────────────────────────────────────────────────────────────────────
  private def demoAbrupt(): Unit = {
    val pool      = Executors.newFixedThreadPool(2)
    val completed = new AtomicInteger(0)
    val cancelled = new AtomicInteger(0)

    println("  [main] Submitting 5 tasks to a 2-thread pool ...")
    (1 to 5).foreach { i =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          println(s"  [Task-$i][${Thread.currentThread().getName}] started")
          try {
            Thread.sleep(2000)   // long sleep -- will be interrupted
            completed.incrementAndGet()
            println(s"  [Task-$i] finished normally (unexpected in this demo)")
          } catch {
            case _: InterruptedException =>
              cancelled.incrementAndGet()
              println(s"  [Task-$i][${Thread.currentThread().getName}] interrupted by shutdownNow()")
              Thread.currentThread().interrupt()  // restore flag for worker loop
          }
        }
      })
    }

    Thread.sleep(200)  // let tasks 1 & 2 enter their sleep

    println()
    println("  [main] calling shutdownNow() -- abrupt")
    val neverStarted: List[Runnable] = pool.shutdownNow().asScala.toList
    println(s"  [main] shutdownNow() returned ${neverStarted.size} un-started tasks")
    println(s"  [main] isShutdown=${pool.isShutdown}  isTerminated=${pool.isTerminated}")

    val terminated = pool.awaitTermination(3, TimeUnit.SECONDS)
    println()
    println(s"  Results:")
    println(s"    Tasks that STARTED   : 2   (occupied the 2 threads)")
    println(s"Subimtted ")
    println(s"    Tasks INTERRUPTED    : ${cancelled.get()}   (running tasks caught InterruptedException)")
    println(s"    Tasks COMPLETED norm : ${completed.get()}   (none -- all were interrupted)")
    println(s"    Tasks NEVER started  : ${neverStarted.size}   (returned by shutdownNow())")
    println(s"    Terminated           : $terminated")
    println("  ABRUPT: running tasks interrupted; un-started tasks handed back to caller.")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 3. State transitions
  //
  //   RUNNING    -- pool operating normally
  //   SHUTDOWN   -- shutdown()/shutdownNow() called (isShutdown=true)
  //   TERMINATED -- all threads stopped (isTerminated=true)
  //
  //   isShutdown()   returns true in both SHUTDOWN and TERMINATED states.
  //   isTerminated() returns true ONLY in TERMINATED state.
  //   awaitTermination() blocks until TERMINATED or timeout.
  // ─────────────────────────────────────────────────────────────────────────
  private def demoStates(): Unit = {
    val pool = Executors.newFixedThreadPool(1)

    def printState(label: String): Unit =
      println(f"  [$label%-30s]  isShutdown=${pool.isShutdown}  isTerminated=${pool.isTerminated}")

    printState("INITIAL (running)")

    pool.submit(new Runnable {
      def run(): Unit = { Thread.sleep(400) }
    })

    printState("after submit, task running")

    pool.shutdown()
    printState("after shutdown() called")
    // isShutdown=true, but the task is still running so isTerminated=false

    val step1 = pool.awaitTermination(100, TimeUnit.MILLISECONDS)
    printState(s"awaitTermination(100ms)=$step1")  // false -- task still running

    val step2 = pool.awaitTermination(600, TimeUnit.MILLISECONDS)
    printState(s"awaitTermination(600ms)=$step2")  // true -- task finished by now

    println()
    println("  Key rule: isShutdown does NOT mean work is done.")
    println("            isTerminated is the definitive 'everything stopped' signal.")
    println("            Always pair shutdown() with awaitTermination() to block until idle.")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 4. Rescue -- tasks returned by shutdownNow() can be re-submitted
  //
  //   The returned List[Runnable] are the original Runnable objects, still
  //   callable.  A common pattern: submit them to a fresh replacement pool,
  //   or persist/log them so no work is silently lost.
  // ─────────────────────────────────────────────────────────────────────────
  private def demoRescue(): Unit = {
    val completedIn = new ConcurrentHashMap[String, String]()
    // Tag each pool so we can tell in the output which pool ran the task.
    val originalFactory = namedFactory("original")
    val rescueFactory   = namedFactory("rescue")
    val taggedOriginal  = Executors.newFixedThreadPool(2, originalFactory)
    val taggedRescue    = Executors.newFixedThreadPool(2, rescueFactory)

    def makeTask(i: Int): Runnable = new Runnable {
      override def run(): Unit = {
        val name = Thread.currentThread().getName
        try {
          Thread.sleep(300)                     // long enough to stay in-flight at 50ms
          completedIn.put(s"task-$i", name)
          println(s"  [Task-$i][$name] completed")
        } catch {
          case _: InterruptedException =>
            println(s"  [Task-$i][$name] interrupted (running task)")
            Thread.currentThread().interrupt()
        }
      }
    }

    println("  [main] Submitting 5 tasks to original pool (2 threads) ...")
    (1 to 5).foreach { i => taggedOriginal.submit(makeTask(i)) }

    Thread.sleep(50)   // tasks 1 & 2 are running; tasks 3, 4, 5 sit in queue
    println("  [main] Aborting original pool with shutdownNow() ...")
    val orphans: List[Runnable] = taggedOriginal.shutdownNow().asScala.toList
    println(s"  [main] ${orphans.size} orphaned tasks returned (were still in queue)")

    // Re-submit every un-started task to the rescue pool so no work is lost
    println(s"  [main] Re-submitting ${orphans.size} orphaned tasks to rescue pool ...")
    orphans.foreach(taggedRescue.submit)

    taggedOriginal.awaitTermination(1, TimeUnit.SECONDS)
    taggedRescue.shutdown()
    taggedRescue.awaitTermination(5, TimeUnit.SECONDS)

    println()
    println(s"  Tasks completed: ${completedIn.size()} / 5")
    println("  RESCUE: un-started tasks re-submitted to a fresh pool -- no work lost.")
  }

  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val id = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${id.getAndIncrement()}")
  }

  private def section(n: Int, title: String): Unit = {
    println()
    println("─" * 70)
    println(s"  [$n] $title")
    println("─" * 70)
  }
}
