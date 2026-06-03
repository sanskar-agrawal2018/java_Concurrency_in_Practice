/**
 * Chapter 7 -- Bulk Task Cancellation Demo
 *
 * Setup: 3-thread pool, 5 tasks submitted, each sleeps for 10 seconds.
 *        Pool can only run 3 at a time, so 2 tasks sit in the queue.
 *
 * Goal:  Cancel EVERYTHING -- tasks currently running AND tasks waiting.
 *
 * Two approaches demonstrated:
 *
 *   APPROACH 1 -- shutdownNow()
 *     - Interrupts threads running the 3 active tasks.
 *     - Drains the 2 queued tasks from the pool's work queue and returns them.
 *     - Shuts the pool down (no new work accepted after this point).
 *     - Tasks must handle InterruptedException or they keep running!
 *
 *   APPROACH 2 -- Future.cancel(true) on each submitted future
 *     - Cancels queued tasks (they never start).
 *     - Interrupts the thread for tasks that are already running.
 *     - Pool stays alive; you can submit new work afterwards.
 *     - Same rule: running tasks must cooperate with interruption.
 *
 *   CONTRAST -- what happens when a task SWALLOWS InterruptedException
 *     - shutdownNow() interrupts the thread, but the task clears the flag
 *       and keeps sleeping.  Thread pool cannot terminate until the task
 *       eventually finishes on its own.
 *
 * Run:
 *   sbt "chapter7/runMain BulkTaskCancellationDemo"
 */

import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object BulkTaskCancellationDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("APPROACH 1 -- shutdownNow(): interrupt running + drain queued")
    println("=" * 70)
    approachShutdownNow()

    println()
    println("=" * 70)
    println("APPROACH 2 -- Future.cancel(true) on each future")
    println("=" * 70)
    approachFutureCancel()

    println()
    println("=" * 70)
    println("CONTRAST  -- bad tasks swallow InterruptedException; pool hangs")
    println("=" * 70)
    contrastSwallowedInterrupt()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // APPROACH 1 -- shutdownNow()
  // ─────────────────────────────────────────────────────────────────────────
  private def approachShutdownNow(): Unit = {
    val pool = Executors.newFixedThreadPool(3)

    // Each task announces start, sleeps 10 s, and handles interruption properly.
    val submitted = new AtomicInteger(0)
    val started   = new AtomicInteger(0)
    val stopped   = new AtomicInteger(0)

    val tasks: List[Runnable] = (1 to 5).map { i =>
      val r: Runnable = () => {
        started.incrementAndGet()
        println(s"  [Task-$i][${Thread.currentThread().getName}] RUNNING -- sleeping 10 s")
        try {
          Thread.sleep(10_000)
          println(s"  [Task-$i] completed normally (should not print)")
        } catch {
          case _: InterruptedException =>
            stopped.incrementAndGet()
            println(s"  [Task-$i][${Thread.currentThread().getName}] INTERRUPTED -- exiting cleanly; restoring flag")
            Thread.currentThread().interrupt()  // restore for worker loop (Recipient 2)
        }
      }
      r
    }.toList

    println(s"  Submitting 5 tasks to a 3-thread pool ...")
    tasks.foreach { t => pool.submit(t); submitted.incrementAndGet() }
    println(s"  Submitted ${submitted.get()} tasks; pool has 3 threads so 2 sit in queue")

    Thread.sleep(400)   // let 3 tasks start running inside their sleep

    println()
    println("  [main] Calling pool.shutdownNow() ...")
    val neverStarted: java.util.List[Runnable] = pool.shutdownNow()
    //  shutdownNow() does TWO things:
    //    1. Interrupts every thread currently running a task.
    //    2. Drains the remaining queue and returns those Runnables.
    println(s"  [main] shutdownNow() returned ${neverStarted.size()} tasks that NEVER started (were in queue)")
    println(s"  [main] Those ${neverStarted.size()} queued tasks are simply discarded -- no thread was ever assigned to them")

    val terminated = pool.awaitTermination(3, TimeUnit.SECONDS)
    println()
    println(s"  Results:")
    println(s"    Tasks submitted  : ${submitted.get()}")
    println(s"    Tasks that ran   : ${started.get()}  (3 threads * 1 task each)")
    println(s"    Tasks interrupted: ${stopped.get()}  (running tasks that caught InterruptedException)")
    println(s"    Tasks in queue   : ${neverStarted.size()}  (never assigned a thread)")
    println(s"    Pool terminated  : $terminated")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // APPROACH 2 -- Future.cancel(true) on each submitted future
  //
  // Key difference from shutdownNow():
  //   - The POOL stays alive; you can submit new work after cancellation.
  //   - cancel(true) on a QUEUED future removes it from the pool's queue.
  //   - cancel(true) on a RUNNING future interrupts its thread.
  // ─────────────────────────────────────────────────────────────────────────
  private def approachFutureCancel(): Unit = {
    val pool = Executors.newFixedThreadPool(3)

    val started = new AtomicInteger(0)
    val stopped = new AtomicInteger(0)

    val futures: List[Future[_]] = (1 to 5).map { i =>
      pool.submit(new Callable[Unit] {
        override def call(): Unit = {
          started.incrementAndGet()
          println(s"  [Task-$i][${Thread.currentThread().getName}] RUNNING -- sleeping 10 s")
          try {
            Thread.sleep(10_000)
            println(s"  [Task-$i] completed normally (should not print)")
          } catch {
            case _: InterruptedException =>
              stopped.incrementAndGet()
              println(s"  [Task-$i][${Thread.currentThread().getName}] INTERRUPTED via future.cancel(true)")
              Thread.currentThread().interrupt()
          }
        }
      })
    }.toList

    println(s"  Submitted 5 tasks (3 running, 2 queued)")
    Thread.sleep(400)   // let 3 tasks get into their sleep

    println()
    println("  [main] Calling future.cancel(true) on every future ...")
    val results = futures.zipWithIndex.map { case (f, i) =>
      val taskNum   = i + 1
      val cancelled = f.cancel(true)
      //  cancel(true) returns:
      //    true  -- task was queued (removed) OR running (thread interrupted)
      //    false -- task had already completed or was already cancelled
      val state = if (f.isCancelled) "cancelled" else if (f.isDone) "done" else "still-running?"
      println(s"  [Task-$taskNum] cancel(true) returned=$cancelled  state=$state")
      cancelled
    }
    println(s"  [main] ${results.count(_ == true)}/5 futures successfully cancelled")

    pool.awaitTermination(3, TimeUnit.SECONDS)
    println()
    println(s"  Results:")
    println(s"    Tasks that started : ${started.get()}")
    println(s"    Tasks interrupted  : ${stopped.get()}")
    println(s"    Pool still alive   : ${!pool.isShutdown}  (pool was NOT shut down -- can accept new work)")

    // Prove the pool is still usable
    val lateResult = pool.submit(new Callable[String] {
      override def call(): String = s"late-task ran on ${Thread.currentThread().getName}"
    })
    println(s"  [main] Submitted a new task after cancellation: ${lateResult.get(2, TimeUnit.SECONDS)}")
    pool.shutdown()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // CONTRAST -- bad tasks that swallow InterruptedException
  //
  // shutdownNow() interrupts the thread, but the bad task catches
  // InterruptedException, CLEARS the flag, and goes back to sleep.
  // The pool cannot terminate until the full 10-second sleep finishes.
  // ─────────────────────────────────────────────────────────────────────────
  private def contrastSwallowedInterrupt(): Unit = {
    val pool = Executors.newFixedThreadPool(3)

    (1 to 5 ).foreach { i =>    // just 3 tasks so all are running immediately
      pool.submit(new Callable[Unit] {
        val name = s"BadTask-$i"
        override def call(): Unit = {
          println(s"  [BadTask-$i][${Thread.currentThread().getName}] started; sleeping 10 s in a loop")
          // Simulates a task that retries sleep on InterruptedException -- a common bug
          var remaining = 10_000L
          val deadline  = System.currentTimeMillis() + remaining
          while (remaining > 0) {
            try {
              Thread.sleep(remaining)
              remaining = 0
            } catch {
              case _: InterruptedException =>
                // BUG: swallowing the interrupt and recalculating remaining time
                remaining = deadline - System.currentTimeMillis()
                if (remaining > 0)
                  println(s"  [BadTask-$i] swallowed InterruptedException; ${remaining}ms left -- KEEPS SLEEPING!")
                // Flag is now cleared; pool worker loop won't see it
            }
          }
          println(s"  [BadTask-$i] finished after full 10 s despite shutdownNow()")
        }
      })
    }

    Thread.sleep(400)
    val start = System.currentTimeMillis()
    println(s"  [main] calling shutdownNow() -- bad tasks will swallow the interrupt ...")
    val notStatedTask =pool.shutdownNow()
    notStatedTask.forEach(t => println(s"  [main] shutdownNow() returned a task that never started: ${ }"))
    // awaitTermination will time out because bad tasks keep sleeping
    val terminated = pool.awaitTermination(2, TimeUnit.SECONDS)
    val elapsed    = System.currentTimeMillis() - start
    println(s"  [main] awaitTermination(2s) returned=$terminated after ${elapsed}ms")
    if (!terminated)
      println(s"  [main] Pool did NOT terminate! Bad tasks are still sleeping -- resource leak.")

    pool.shutdownNow()   // second attempt; still won't help if tasks keep swallowing
    pool.awaitTermination(500, TimeUnit.MILLISECONDS)
    println(s"  [main] Lesson: shutdownNow() can only SIGNAL interruption.")
    println(s"         A task that swallows InterruptedException makes the pool impossible to stop promptly.")
  }
}
