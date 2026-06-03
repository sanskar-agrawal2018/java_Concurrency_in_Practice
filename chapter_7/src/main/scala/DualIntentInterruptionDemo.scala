/**
 * Chapter 7 -- Dual-intent interruption demo
 *
 * The problem this file demonstrates:
 *
 *   A single Thread.interrupt() on a pool worker thread has TWO potential
 *   recipients with different expectations:
 *
 *     Recipient 1 -- the TASK currently running on that thread.
 *                    It wants to stop its own work (cancel).
 *
 *     Recipient 2 -- the WORKER LOOP (the thread itself).
 *                    The pool expects it to exit when the pool shuts down.
 *
 *   When ExecutorService.shutdownNow() is called it interrupts every running
 *   thread.  The intent is for BOTH the task AND the worker loop to react.
 *
 *   If the task catches InterruptedException and does NOT restore the flag:
 *     - The flag is cleared.
 *     - The worker loop never sees the interrupt.
 *     - The pool cannot terminate promptly; it may keep accepting new work.
 *
 *   If the task catches InterruptedException and DOES restore the flag before
 *   returning (Thread.currentThread().interrupt()):
 *     - The task stops.
 *     - The worker loop detects the flag on its next iteration check.
 *     - The pool terminates as expected.
 *
 * This demo uses a hand-rolled MinimalWorkerThread (a visible worker loop)
 * so you can watch the two-recipient dynamic happen in both demos.
 *
 * Run:
 *   sbt "chapter7/runMain DualIntentInterruptionDemo"
 */

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

object DualIntentInterruptionDemo {

  def main(args: Array[String]): Unit = {
    demoBadTask()
    println()
    println("=" * 90)
    println()
//    demoGoodTask()
  }

  // --------------------------------------------------------------------------
  // Demo 1: Task swallows interrupt -- worker loop never sees it
  // --------------------------------------------------------------------------
  private def demoBadTask(): Unit = {
    println("DEMO 1: task SWALLOWS interrupt -- only the task may stop; worker loop stays alive")
    println("=" * 90)
    println(
      "When ExecutorService.shutdownNow() interrupts a worker, BOTH the task and the\n" +
      "worker loop are supposed to react.  If the task clears the flag the loop is blind."
    )
    println()

    val taskQueue = new ArrayBlockingQueue[Runnable](8)
    val pool = new MinimalThreadPool("bad-pool", threads = 1, taskQueue)
    pool.start()

    val taskStarted  = new CountDownLatch(1)
    val taskFinished = new CountDownLatch(1)
    val itemsProcessed = new AtomicInteger(0)

    // The bad task: catches InterruptedException but does NOT restore the flag.
    // It stops doing work (fine), but leaves the worker loop oblivious to the interrupt.
    val badTask: Runnable = () => {
      println(s"[${Thread.currentThread().getName}] bad-task started; will swallow interrupt")
      taskStarted.countDown()
      var keepProcessing = true
      while (keepProcessing) {
        try {
          Thread.sleep(100)
          val n = itemsProcessed.incrementAndGet()
          println(s"[${Thread.currentThread().getName}] bad-task processed item #$n")
        }
      }
      println(s"[${Thread.currentThread().getName}] bad-task exiting; isInterrupted=${Thread.currentThread().isInterrupted}")
      taskFinished.countDown()
    }

    pool.submit(badTask)
    taskStarted.await()

    // Let the task do a few iterations.
    Thread.sleep(350)

    println()
    println("[main] Calling pool.shutdownNow() -- interrupts worker thread")
    println("[main] Intent: stop the task (Recipient 1) AND stop the worker loop (Recipient 2)")
    pool.shutdownNow()
    pool.printShutDown()

    // Wait for the task itself to exit.
    taskFinished.await(2, TimeUnit.SECONDS)

    // Now check whether the worker loop also stopped.
    val poolDied = pool.awaitTermination(500, TimeUnit.MILLISECONDS)
    println()
    println(s"[main] bad-task stopped    : ${taskFinished.await(0, TimeUnit.MILLISECONDS)}")
    println(s"[main] worker loop stopped : $poolDied")
    println(s"[main] items processed     : ${itemsProcessed.get()}")
    if (!poolDied) {
      println(
        "[main] Worker loop is STILL RUNNING despite shutdownNow()!\n" +
        "[main] The pool queues more tasks below -- the dead-but-alive worker will process them."
      )
      // Enqueue a late task to prove the worker loop is still alive.
      val lateProcessed = new AtomicInteger(0)
      pool.submit(() => {
        lateProcessed.incrementAndGet()
        println(s"[${Thread.currentThread().getName}] LATE TASK ran after shutdownNow() -- pool did not die!")
      })
      Thread.sleep(300)
      println(s"[main] late tasks processed: ${lateProcessed.get()}")
      pool.forceStop()
    }
    println("[main] Demo 1 complete.")
  }

  // --------------------------------------------------------------------------
  // Demo 2: Task restores interrupt -- worker loop sees it and exits
  // --------------------------------------------------------------------------
  private def demoGoodTask(): Unit = {
    println("DEMO 2: task RESTORES interrupt flag -- both task and worker loop stop promptly")
    println("=" * 90)
    println(
      "The task calls Thread.currentThread().interrupt() before returning so the\n" +
      "worker loop's isInterrupted() check fires on the very next iteration."
    )
    println()

    val taskQueue = new ArrayBlockingQueue[Runnable](8)
    val pool = new MinimalThreadPool("good-pool", threads = 1, taskQueue)
    pool.start()

    val taskStarted  = new CountDownLatch(1)
    val taskFinished = new CountDownLatch(1)
    val itemsProcessed = new AtomicInteger(0)

    // The good task: catches InterruptedException, restores the flag, then exits.
    // The worker loop will observe the flag on its next check and also exit.
    val goodTask: Runnable = () => {
      println(s"[${Thread.currentThread().getName}] good-task started; will restore interrupt")
      taskStarted.countDown()
      var keepProcessing = true
      while (keepProcessing) {
        try {
          Thread.sleep(100)
          val n = itemsProcessed.incrementAndGet()
          println(s"[${Thread.currentThread().getName}] good-task processed item #$n")
        } catch {
          case _: InterruptedException =>
            println(
              s"[${Thread.currentThread().getName}] good-task caught InterruptedException -- " +
              s"restoring flag before exiting"
            )
            // KEY LINE: re-set the interrupt flag so the worker loop also sees it.
            Thread.currentThread().interrupt()
            println(
              s"[${Thread.currentThread().getName}] good-task isInterrupted after restore=" +
              s"${Thread.currentThread().isInterrupted}"
            )
            keepProcessing = false
        }
      }
      println(s"[${Thread.currentThread().getName}] good-task exiting cleanly")
      taskFinished.countDown()
      // After this method returns, the worker loop checks isInterrupted() and exits too.
    }

    pool.submit(goodTask)
    taskStarted.await()

    // Let the task do a few iterations.
    Thread.sleep(350)

    println()
    println("[main] Calling pool.shutdownNow() -- interrupts worker thread")
    pool.shutdownNow()

    taskFinished.await(2, TimeUnit.SECONDS)

    val poolDied = pool.awaitTermination(1, TimeUnit.SECONDS)
    println()
    println(s"[main] good-task stopped    : ${taskFinished.await(0, TimeUnit.MILLISECONDS)}")
    println(s"[main] worker loop stopped  : $poolDied")
    println(s"[main] items processed      : ${itemsProcessed.get()}")

    if (poolDied) {
      // Enqueue a late task to confirm the worker loop is truly dead.
      val lateProcessed = new AtomicInteger(0)
      pool.submit(() => {
        lateProcessed.incrementAndGet()
        println("[late-task] this should NOT print")
      })
      Thread.sleep(300)
      println(s"[main] late tasks processed : ${lateProcessed.get()} (expected 0)")
      println("[main] Worker loop is dead. Late work was NOT processed. Correct!")
    } else {
      println("[main] Worker loop did not stop -- unexpected for the good-task demo!")
    }
    println("[main] Demo 2 complete.")
  }
}

// ----------------------------------------------------------------------------
// MinimalThreadPool -- a hand-rolled single-level thread pool whose worker
// loop is visible so we can observe the two-recipient interrupt dynamic.
// ----------------------------------------------------------------------------
final class MinimalThreadPool(
    name: String,
    threads: Int,
    taskQueue: BlockingQueue[Runnable]
) {
  private val shutdown  = new AtomicBoolean(false)
  private val workers   = Array.tabulate(threads)(i => new WorkerThread(s"$name-worker-$i"))

  def start(): Unit = workers.foreach(_.start())

  def submit(task: Runnable): Boolean = {
    if (shutdown.get()) {
      println(s"[MinimalThreadPool] submit rejected -- pool is shut down")
      false
    } else {
      taskQueue.offer(task)
    }
  }


  def printShutDown():Unit ={
    val shut_Var=shutdown.get()
    println(s"[Thread: ${Thread.currentThread().getName}] Value of shutDown =${shut_Var}")
  }

  /**
   * Signals shutdown and interrupts all running workers.
   * The shutdown flag only gates new submissions; the worker loop itself
   * relies SOLELY on the interrupt flag to decide when to stop.  This is
   * intentional: it makes the two-recipient bug observable -- if a task
   * swallows the interrupt, the worker loop never sees it and keeps running.
   */
  def shutdownNow(): Unit = {
    shutdown.set(true)             // blocks new submissions
    workers.foreach(x=> {
        println(s"[MinimalThreadPool][${Thread.currentThread().getName}][${System.currentTimeMillis()}] shutdownNow: interrupting worker ${x.getName} -- this is the ONLY signal the worker loop reacts to")
        x.interrupt()
        println(s"[MinimalThreadPool][${Thread.currentThread().getName}][${System.currentTimeMillis()}] shutdownNow: interrupted worker ${x.getName} Is Interrupted :${x.isInterrupted} -- this is the ONLY signal the worker loop reacts to")
      }
    ) // the ONLY signal the worker loop reacts to
    printShutDown()
    println(s"[MinimalThreadPool] shutdownNow: interrupted ${workers.length} worker(s); new submissions blocked")
  }

  /** Waits up to `timeout` for all workers to die. */
  def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    val deadline = System.nanoTime() + unit.toNanos(timeout)
    workers.forall { w =>
      val remaining = deadline - System.nanoTime()
      if (remaining <= 0) false
      else { w.join(TimeUnit.NANOSECONDS.toMillis(remaining));println(s"Worker print ${!w.isAlive }"); !w.isAlive }
    }
  }

  def forceStop(): Unit = workers.foreach(_.interrupt())

  // Inner worker: picks tasks from the queue and runs them.
  // After each task it checks Thread.isInterrupted() -- this is Recipient 2.
  private class WorkerThread(threadName: String) extends Thread(threadName) {
    override def run(): Unit = {
      println(s"[$threadName] worker loop started")
      try {
        // The loop continues as long as the thread is NOT interrupted.
        // NOTE: we deliberately do NOT check shutdown.get() here.
        // The ONLY way this loop should stop is via the interrupt flag.
        // This exposes the two-recipient bug: if a task swallows the
        // InterruptedException without restoring the flag, this condition
        // is never false and the worker keeps running indefinitely.
        while (!Thread.currentThread().isInterrupted) {
          println(s"[$threadName][${Thread.currentThread().getName}][${System.currentTimeMillis()}] worker loop: waiting for task; isInterrupted=${Thread.currentThread().isInterrupted}")
          val task = taskQueue.poll(200, TimeUnit.MILLISECONDS)
          if (task != null) {
            println(s"[$threadName] picked up task; isInterrupted before run=${isInterrupted}")
            try {
              task.run()
            } catch {
              case _: InterruptedException =>
                // A task that doesn't handle its own InterruptedException reaches here.
                // Restore the flag so the loop condition fires.
                println(s"[$threadName] task threw InterruptedException up to worker loop -- restoring flag")
                Thread.currentThread().interrupt()
            }
            // *** Recipient 2 check ***
            // If the task restored the interrupted flag, this condition is true
            // and the worker loop exits -- pool shuts down correctly.
            // If the task swallowed the flag, this is false and the loop continues.
            if (Thread.currentThread().isInterrupted) {
              println(s"[$threadName] post-task: isInterrupted=true -> worker loop will exit (Recipient 2 handled)")
            } else if (shutdown.get()) {
              println(s"[$threadName] post-task: shutdown flag set -> worker loop exits")
            }
          }
        }
      } catch {
        case _: InterruptedException =>
          // poll() was interrupted while waiting (no task was running).
          println(s"[$threadName][${System.currentTimeMillis()}]  worker interrupted while idle -- exiting worker loop")
          Thread.currentThread().interrupt()
      } finally {
        println(s"[$threadName][${System.currentTimeMillis()}]  worker loop finished; isInterrupted=${Thread.currentThread().isInterrupted}")
      }
    }
  }
}
