/**
 * Chapter 7 -- Listing 7.6: Propagating InterruptedException
 *
 * Propagating InterruptedException can be as easy as adding
 * InterruptedException to the throws clause, as shown by getNextTask
 * in Listing 7.6. When a method cannot handle the interruption itself
 * (because it is not the right layer to decide what "stop" means), the
 * cleanest option is to let the exception propagate to the caller who
 * knows how to react.
 *
 * This demo contrasts two approaches:
 *
 *   BAD  -- getNextTaskBad() catches InterruptedException and returns null.
 *            The worker loop can't distinguish "no task" from "interrupted",
 *            the flag is cleared, and the thread keeps running.
 *
 *   GOOD -- getNextTaskGood() declares @throws[InterruptedException] and
 *            lets BlockingQueue.take() propagate naturally.  The worker loop
 *            receives the exception, restores the flag, and exits cleanly.
 *
 * Run:
 *   sbt "chapter7/runMain PropagatingInterruptedExceptionDemo"
 */

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

object PropagatingInterruptedExceptionDemo {

  def main(args: Array[String]): Unit = {
    println("=== Listing 7.6: Propagating InterruptedException ===")
    println()

    println("--- DEMO 1: BAD  -- InterruptedException swallowed inside getNextTask ---")
    demoBad()
    println()

    println("--- DEMO 2: GOOD -- InterruptedException propagated by getNextTask ---")
    demoGood()
    println()

    println("=== Done ===")
  }

  // ---------------------------------------------------------------------------
  // DEMO 1 -- bad: InterruptedException is caught and suppressed inside
  //           getNextTask. The flag is cleared; the caller never knows.
  // ---------------------------------------------------------------------------
  private def demoBad(): Unit = {
    val queue  = new TaskQueue
    val worker = new Thread(new BadWorker(queue), "bad-worker")
    worker.start()

    // Feed a few tasks then interrupt after 600 ms.
    feedTasks(queue, count = 3, delayMs = 150)
    Thread.sleep(600)

    println(s"[main][BAD] interrupting bad-worker")
    worker.interrupt()

    worker.join(2000)
    if (worker.isAlive) {
      println(s"[main][BAD] bad-worker is STILL ALIVE after 2 s -- interrupt was swallowed!")
      worker.interrupt() // force-stop so demo can continue
      worker.join(500)
    } else {
      println(s"[main][BAD] bad-worker stopped (isAlive=false)")
    }
  }

  // ---------------------------------------------------------------------------
  // DEMO 2 -- good: getNextTask propagates InterruptedException; the worker
  //           loop catches it at the right level and exits cleanly.
  // ---------------------------------------------------------------------------
  private def demoGood(): Unit = {
    val queue  = new TaskQueue
    val worker = new Thread(new GoodWorker(queue), "good-worker")
    worker.start()

    // Feed a few tasks then interrupt after 600 ms.
    feedTasks(queue, count = 3, delayMs = 150)
    Thread.sleep(600)

    println(s"[main][GOOD] interrupting good-worker")
    worker.interrupt()

    worker.join(2000)
    if (worker.isAlive)
      println(s"[main][GOOD] good-worker is STILL ALIVE -- unexpected!")
    else
      println(s"[main][GOOD] good-worker stopped cleanly")
  }

  private def feedTasks(queue: TaskQueue, count: Int, delayMs: Long): Unit = {
    new Thread(() => {
      (1 to count).foreach { i =>
        Thread.sleep(delayMs)
        queue.put(() => println(s"[${Thread.currentThread().getName}] executed task #$i"))
      }
    }, "task-feeder").start()
  }
}

// -----------------------------------------------------------------------------
// TaskQueue -- wraps a LinkedBlockingQueue and exposes two flavours of
// getNextTask to illustrate the propagation difference.
// -----------------------------------------------------------------------------
final class TaskQueue {
  private val queue = new LinkedBlockingQueue[Runnable]()

  def put(task: Runnable): Unit = queue.put(task)

  /**
   * BAD: catches InterruptedException internally and returns null.
   * The interrupt flag is cleared and the caller has no way to know
   * the thread was interrupted -- it just sees a null result.
   */
  def getNextTaskBad(): Runnable = {
    try {
      queue.take()
    } catch {
      case _: InterruptedException =>
        // Flag is cleared here; caller is oblivious.
        println(s"[${Thread.currentThread().getName}][BAD][getNextTask] caught InterruptedException -- SWALLOWING IT, returning null")
        null
    }
  }

  /**
   * GOOD (Listing 7.6): propagates InterruptedException to the caller.
   * BlockingQueue.take() already throws it; we simply do not catch it.
   * The @throws annotation is optional in Scala but documents the contract.
   */
  @throws[InterruptedException]
  def getNextTaskGood(): Runnable = queue.take()
}

// -----------------------------------------------------------------------------
// BadWorker -- calls getNextTaskBad(). When interrupted the flag is cleared
// inside getNextTask; the while-loop condition is never false so the thread
// keeps running even after interruption.
// -----------------------------------------------------------------------------
final class BadWorker(queue: TaskQueue) extends Runnable {
  override def run(): Unit = {
    println(s"[${Thread.currentThread().getName}] bad-worker started")
    var iterations = 0

    // We cap at 20 iterations so the demo eventually ends even though the
    // interrupt is swallowed -- without the cap the thread would run forever.
    while (iterations < 20) {
      iterations += 1
      println(s"[${Thread.currentThread().getName}][BAD] waiting for task (iter=$iterations); isInterrupted=${Thread.currentThread().isInterrupted}")
      val task = queue.getNextTaskBad()
      if (task != null) {
        task.run()
      } else {
        // null means the exception was swallowed -- the flag has been cleared.
        println(s"[${Thread.currentThread().getName}][BAD] getNextTask returned null (interrupt was swallowed); flag=${Thread.currentThread().isInterrupted}")
        // The worker CANNOT distinguish "interrupted" from "empty queue",
        // so it has no choice but to keep looping or give up via an
        // ad-hoc mechanism -- neither is clean.
        return // we exit manually here only to end the demo
      }
    }
    println(s"[${Thread.currentThread().getName}][BAD] bad-worker loop finished")
  }
}

// -----------------------------------------------------------------------------
// GoodWorker -- calls getNextTaskGood() which propagates InterruptedException.
// The exception surfaces here, where the worker can handle it correctly:
// restore the flag and exit the loop.
// -----------------------------------------------------------------------------
final class GoodWorker(queue: TaskQueue) extends Runnable {
  override def run(): Unit = {
    println(s"[${Thread.currentThread().getName}] good-worker started")
    try {
      while (!Thread.currentThread().isInterrupted) {
        println(s"[${Thread.currentThread().getName}][GOOD] waiting for task; isInterrupted=${Thread.currentThread().isInterrupted}")
        // InterruptedException propagates from queue.take() through
        // getNextTaskGood() all the way up to this catch block -- the
        // right layer to decide what "stop" means.
        val task = queue.getNextTaskGood()
        task.run()
      }
    } catch {
      case _: InterruptedException =>
        // This is the correct place to handle the exception.
        // Restore the flag so any code above this point (e.g. a thread pool
        // worker loop) can also observe the interruption.
        Thread.currentThread().interrupt()
        println(
          s"[${Thread.currentThread().getName}][GOOD] InterruptedException propagated from getNextTask -- " +
          s"restoring flag (isInterrupted=${Thread.currentThread().isInterrupted}) and exiting"
        )
    }
    println(s"[${Thread.currentThread().getName}][GOOD] good-worker exiting cleanly")
  }
}
