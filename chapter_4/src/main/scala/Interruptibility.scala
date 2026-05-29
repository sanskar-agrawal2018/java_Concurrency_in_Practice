/*
 * Interruptibility in Scala  (JVM thread interruption)
 *
 * A running thread CANNOT be safely force-killed (Thread.stop is deprecated and
 * dangerous -- it can leave shared state half-updated). Instead, the JVM offers
 * INTERRUPTION: a cooperative "please stop" request that the target thread is
 * expected to notice and honor at a convenient point.
 *
 * The mechanism is a single boolean per thread -- the "interrupt status" flag:
 *
 *   t.interrupt()              -- sets t's flag. Just a request; t keeps running.
 *   t.isInterrupted()          -- READS the flag, leaves it set (instance method).
 *   Thread.interrupted()       -- READS the flag AND CLEARS it (static; acts on
 *                                 the *current* thread).
 *
 * Two kinds of code react to the flag very differently:
 *
 *   1. BLOCKING calls -- Thread.sleep, Object.wait, Thread.join, and
 *      BlockingQueue.put/take. These watch the flag for you: if it is (or
 *      becomes) set, they throw InterruptedException AND clear the flag.
 *
 *   2. PLAIN CPU loops -- nothing throws, because nothing is blocking. Such
 *      code must POLL `Thread.currentThread().isInterrupted` itself and stop.
 *
 * Handling InterruptedException -- you have exactly two correct choices:
 *   (a) let it propagate, or
 *   (b) restore the status with `Thread.currentThread().interrupt()` and exit.
 * NEVER silently swallow it -- that destroys the only evidence that someone
 * asked the thread to stop, making the thread effectively un-cancellable.
 *
 * This is exactly why FileCrawler and Indexer in DesktopSearch.scala write:
 *   catch { case _: InterruptedException => Thread.currentThread().interrupt() }
 * -- option (b): stop the loop, but re-raise the flag for callers above.
 */

import com.sun.jna.{Library, Native}

/**
 * JNA binding to the system C library. `sched_getcpu()` is a Linux/glibc call
 * that returns the number of the CPU core the CALLING thread is currently on.
 */
trait CLib extends Library {
  def sched_getcpu(): Int
}

/** Reports the core the current thread is running on (Linux only). */
object Cpu {
  // Loaded once, lazily; None if JNA or the C library is unavailable.
  private lazy val cLib: Option[CLib] =
    try Some(Native.load("c", classOf[CLib]))
    catch { case _: Throwable => None }

  /**
   * The core the CURRENT thread is on, sampled right now (-1 if unavailable).
   * This is only a snapshot: the OS scheduler may move the thread immediately
   * after the call returns.
   */
  def current(): Int =
    cLib.flatMap(l => try Some(l.sched_getcpu()) catch { case _: Throwable => None })
        .getOrElse(-1)
}

object Interruptibility {

  // -------------------------------------------------------------------------
  // Demo A -- a CPU-bound loop: nothing blocks, so we must POLL the flag.
  // -------------------------------------------------------------------------
  private def demoCpuBoundPolling(): Unit = {
    println("[A] CPU-bound loop -- cooperative cancellation by polling isInterrupted")
    val worker = new Thread(() => {
      var iterations = 0L
      // No blocking call in here, so no InterruptedException is ever thrown.
      // The ONLY way to learn about cancellation is to check the flag.
      while (!Thread.currentThread().isInterrupted) iterations += 1
      println(s"[A]   worker saw the flag and stopped after $iterations iterations")
    })
    worker.start()
    Thread.sleep(50)
    worker.interrupt()   // sets the flag; the worker's own check ends the loop
    worker.join()
  }

  // -------------------------------------------------------------------------
  // Demo B -- a thread blocked in Thread.sleep: interrupt() makes it throw.
  // -------------------------------------------------------------------------
  private def demoBlockingThrows(): Unit = {
    println("[B] blocked thread -- interrupt() turns into an InterruptedException")
    val sleeper = new Thread(() => {
      try {
        println("[B]   sleeper about to sleep for 10s")
        Thread.sleep(10000)
        println("[B]   sleeper woke up normally (we never reach this)")
      } catch {
        case _: InterruptedException =>
          // Throwing InterruptedException CLEARED the flag, so right here
          // isInterrupted reads as false.
          val flag = Thread.currentThread().isInterrupted
          println(s"[B]   sleeper interrupted early; flag cleared by the throw -> isInterrupted=$flag")
      }
    })
    sleeper.start()
    Thread.sleep(100)
    sleeper.interrupt()  // sleep() reacts at once instead of waiting 10s
    sleeper.join()
  }

  // -------------------------------------------------------------------------
  // Demo C -- the BUG: swallowing InterruptedException. The task catches it,
  // does nothing, and keeps going, so interrupt() has no lasting effect.
  // -------------------------------------------------------------------------
  private def demoSwallowingBug(): Unit = {
    println("[C] anti-pattern -- swallowing InterruptedException ignores cancellation")
    val stubborn = new Thread(() => {
      var rounds = 0
      while (rounds < 3) {
        try Thread.sleep(150)
        catch {
          // BAD: caught, flag already cleared, and we neither re-raise it nor
          // stop. The loop simply continues -- the thread cannot be cancelled.
          case _: InterruptedException => ()
        }
        rounds += 1
        println(s"[C]   stubborn task ignored the interrupt and finished round $rounds")
      }
    })
    stubborn.start()
    Thread.sleep(100)
    stubborn.interrupt() // requested... but the task throws the request away
    stubborn.join()
    println("[C]   stubborn task ran to completion despite interrupt() -- the BUG")
  }

  // -------------------------------------------------------------------------
  // Demo D -- the FIX: restore the interrupt status, then exit. This is the
  // pattern FileCrawler and Indexer use in DesktopSearch.scala.
  // -------------------------------------------------------------------------
  private def demoRestoreStatus(): Unit = {
    println(s"[D][Core:${Cpu.current()}] correct pattern -- restore the status so callers still see it")
    def cancellableTask(s:String): Int =
      try {

        println(s"[D][Core:${Cpu.current()}][S:${s}]   cancellable task is sleeping for 10s on core ${Cpu.current()}")
        println(s"[D][Core:${Cpu.current()}][S:${s}]   value of interrupt before sleep:- ${Thread.currentThread().isInterrupted}  -- cancellable task is sleeping for 10s on core ${Cpu.current()}")
        Thread.sleep(10000)

          println(s"[D][Core:${Cpu.current()}][S:${s}]   cancellable task is starting on core ${Cpu.current()}")
           // the interruptible blocking point
        val currentTime= System.currentTimeMillis()
        var x=0L
        println(s"[D][Core:${Cpu.current()}][S:${s}][x:$x] Implementation at start of running for thread:${s}   core:${Cpu.current()}")

        while (System.currentTimeMillis() - currentTime < 5000) {
          // Simulate doing some work in a loop, checking for interruption.
          val  currentTime= System.currentTimeMillis()
          while(System.currentTimeMillis() - currentTime < 1000) {
            // do nothing, just burn CPU for a bit to simulate work and keep the thread alive
            val y=0
          }
          x=x+1



          // Simulate doing some work in a loop, checking for interruption.
//          if (Thread.currentThread().isInterrupted) {
//            println(s"[D][Core:${Cpu.current()}][S:${s}]   cancellable task detected interrupt request, exiting loop")
//            return // Exit the method to stop the task.
//          }
          // ... do some work here ...
        }
        println(s"[D][Core:${Cpu.current()}][S:${s}][x:$x] Implementation at end of running for thread:${s}   core:${Cpu.current()}")


        // ... real work would go here ...
        println(s"[D][Core:${Cpu.current()}][S:${s}]   value of interrupt before complete :- ${Thread.currentThread().isInterrupted}  -- cancellable task is sleeping for 10s on core ${Cpu.current()}")
        println(s"[D][Core:${Cpu.current()}][S:${s}]   cancellable task completed successfully")
        x.toInt
      } catch {
        case _: InterruptedException =>
          println(s"[D][Core:${Cpu.current()}][Threadstate: ${Thread.currentThread().getState}][S:${s}] 1  Value of interrupt:- ${Thread.currentThread().isInterrupted} " +
            s" -- cancellable task was interrupted during sleep, restoring interrupt status and exiting")
          Thread.currentThread().interrupt()
          println(s"[D][Core:${Cpu.current()}][S:${s}] 2 Value of interrupt:- ${Thread.currentThread().isInterrupted}  -- cancellable task was interrupted during sleep, restoring interrupt status and exiting")

          -1 // re-raise: don't hide the request
      }

    val good = new Thread(() => {
      cancellableTask("Thread1")
      // Because the task restored it, the flag is set again here -- code above
      // (and the thread's own shutdown logic) can still observe the request.
      val flag = Thread.currentThread().isInterrupted
      println(s"[D][Core:${Cpu.current()}]   task stopped and re-raised the flag -> isInterrupted=$flag")


    })

    println(s"[D][Core:${Cpu.current()}]   main thread is running on core ${Cpu.current()}")
    good.start()
    Thread.sleep(1000)
    val anotherTask = new Thread(() => {
      // Just to show that the main thread is on a different core than the worker.
      println(s"[D][Core:${Cpu.current()}]   main thread is running on core ${Cpu.current()}")
      cancellableTask("Thread2")
    })

//    anotherTask.start()


    good.interrupt()
    good.join()
//    anotherTask.join()
  }

  // -------------------------------------------------------------------------
  // Demo E -- isInterrupted() only READS the flag; Thread.interrupted() reads
  // it AND clears it. Mixing these up is a common source of bugs.
  // -------------------------------------------------------------------------
  private def demoFlagSemantics(): Unit = {
    println(s"[E][core:${Cpu.current()}] isInterrupted() reads; Thread.interrupted() reads-and-clears")
    val flagDemo = new Thread(() => {
      Thread.currentThread().interrupt() // set our own flag, just to inspect it
      // Heavy busy-loop for ~5s, so this thread stays on a CPU while we sample.
      val deadline = System.currentTimeMillis() + 5000
      var x = 0L
      while (System.currentTimeMillis() < deadline) x += 0L // busy loop to simulate work and keep the thread alive

      // Cpu.current() is the core THIS worker thread is on at the instant of the
      // call -- the OS scheduler may migrate it, so the numbers can differ.
      println(s"[E][core:${Cpu.current()}]   after interrupt():       isInterrupted = ${Thread.currentThread().isInterrupted}")
      println(s"[E][core:${Cpu.current()}]   Thread.interrupted() ->  ${Thread.interrupted()}  (and it CLEARS the flag)")
      println(s"[E][core:${Cpu.current()}]   after that call:         isInterrupted = ${Thread.currentThread().isInterrupted}")
    })
    flagDemo.start()
    // Printed from the MAIN thread -- its core is independent of the worker's.
    println(s"[E][core ${Cpu.current()}]   main thread is running on core ${Cpu.current()}")
    flagDemo.join()
  }

  def main(args: Array[String]): Unit = {
//    demoCpuBoundPolling()
//    println()
//    demoBlockingThrows()
//    println()
//    demoSwallowingBug()
//    println()
    demoRestoreStatus()
    println()
//    demoFlagSemantics()
  }
}