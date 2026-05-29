/*
 * Latches  (java.util.concurrent.CountDownLatch)
 *
 * A LATCH is a one-shot gate. It starts closed and stays closed until a counter
 * reaches zero; once it opens, it stays open forever. You cannot re-arm it --
 * if you need a reusable barrier, use CyclicBarrier (see Barriers.scala).
 *
 * The CountDownLatch API has only three operations of interest:
 *
 *   new CountDownLatch(N)   -- create a latch armed with N tokens.
 *   latch.countDown()       -- consume one token (non-blocking; never throws
 *                              if the count is already 0 -- it's a no-op).
 *   latch.await()           -- block until the count reaches 0. Multiple
 *                              threads may await concurrently; ALL wake up
 *                              when the gate opens.
 *
 * Two textbook uses:
 *
 *   1. STARTING GATE -- N workers all call await() on the SAME latch with
 *      count 1; the driver thread then calls countDown() exactly once, and
 *      every worker is released at (nearly) the same instant. Useful to
 *      reduce startup-skew noise in benchmarks.
 *
 *   2. COMPLETION SIGNAL -- the driver creates a latch with count N (one per
 *      worker) and calls await(). Each worker calls countDown() when done.
 *      The driver wakes up only after every worker has finished. This is the
 *      "join a fixed-size group of tasks without keeping the Thread objects"
 *      idiom.
 *
 * Why use a latch instead of join()? With join() you must hold a reference to
 * each Thread/Future. With a latch you only need a shared counter -- the work
 * can be submitted to a pool, scattered across actors, etc.
 */

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

object Latches {

  // -------------------------------------------------------------------------
  // Demo A -- STARTING GATE: release N workers simultaneously.
  // -------------------------------------------------------------------------
  private def demoStartingGate(): Unit = {
    println("[A] CountDownLatch as a starting gate -- all workers released together")
    val N = 4
    val gate = new CountDownLatch(3)// one token guards the gate
    val gate2 = new CountDownLatch(3) // for workers to signal the driver
    val pool = Executors.newFixedThreadPool(N)

    for (i <- 1 to N) pool.submit(new Runnable {
      def run(): Unit = {
        println(s"[A]   worker-$i ready and waiting at the gate")
        gate.await()

//        gate.await()   // every worker parks here
        println(s"[A]   worker-$i started at ${System.currentTimeMillis() % 100000}")
        gate2.countDown()

      }
    })

    Thread.sleep(200)                           // make sure all workers are parked
    println("[A]   driver opens the gate --1")
    gate.countDown()
    gate.countDown()// single countDown wakes them ALL
    gate.countDown()// extra countDown is a no-op, doesn't throw, doesn't re-close the gate

    gate2.await()
    println("[A] Gate2 finished, all workers signaled completion")
    println("[A]   driver observed: all workers released")
    pool.shutdown()
    pool.awaitTermination(2, TimeUnit.SECONDS)
  }

  // -------------------------------------------------------------------------
  // Demo B -- COMPLETION SIGNAL: wait for N tasks to finish without join().
  // -------------------------------------------------------------------------
  private def demoCompletionSignal(): Unit = {
    println("[B] CountDownLatch as a completion signal -- await N finishes")
    val N = 3
    val done = new CountDownLatch(N)            // one token per worker
    val pool = Executors.newFixedThreadPool(N)

    for (i <- 1 to N) pool.submit(new Runnable {
      def run(): Unit = {
        try {
          Thread.sleep(100L * i)                // simulate variable work
          println(s"[B]   worker-$i finished")
        } finally {
          done.countDown()                      // ALWAYS countDown, even on failure
        }
      }
    })

    println("[B]   driver awaiting completion of all workers...")
    done.await()                                // unblocks only after every countDown
    println("[B]   driver observed: all workers complete")

    pool.shutdown()
    pool.awaitTermination(2, TimeUnit.SECONDS)
  }

  // -------------------------------------------------------------------------
  // Demo C -- await(timeout) returns a Boolean instead of blocking forever.
  // Useful when you want to give up rather than hang on a stuck worker.
  // -------------------------------------------------------------------------
  private def demoTimedAwait(): Unit = {
    println("[C] await(timeout) returns false if the count never reaches zero")
    val latch = new CountDownLatch(2)           // we will only countDown ONCE
    val worker = new Thread(() => {
      Thread.sleep(50)
      latch.countDown()                         // count goes from 2 -> 1, never 0
    })
    worker.start()

    val opened = latch.await(300, TimeUnit.MILLISECONDS)
    println(s"[C]   await returned $opened (false means the gate never opened)")
    worker.join()
  }

  // -------------------------------------------------------------------------
  // Demo D -- one-shot semantics: extra countDown() calls are silently ignored.
  // -------------------------------------------------------------------------
  private def demoOneShot(): Unit = {
    println("[D] a latch cannot be re-armed -- extra countDown() is a no-op")
    val latch = new CountDownLatch(1)
    latch.countDown()                           // 1 -> 0, gate opens
    latch.countDown()                           // 0 -> 0, no effect, no throw
    latch.await()                               // returns immediately
    println(s"[D]   count after extras = ${latch.getCount} (still 0; gate stays open)")
  }

  def main(args: Array[String]): Unit = {
    println("[A:Application] - Latches: one-shot gates for thread coordination-1")
    demoStartingGate();    println()
    demoCompletionSignal();println()
    demoTimedAwait();      println()
    demoOneShot()
  }
}
