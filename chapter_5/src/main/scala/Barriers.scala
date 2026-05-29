/*
 * Barriers  (java.util.concurrent.CyclicBarrier)
 *
 * A BARRIER coordinates a FIXED-SIZE group of threads so that none of them
 * proceeds past a synchronization point until ALL of them have arrived. Unlike
 * CountDownLatch (see Latches.scala), a CyclicBarrier is REUSABLE -- once the
 * last thread arrives, the barrier "trips", everyone is released, and the
 * counter resets to the original party size for the next round.
 *
 *   new CyclicBarrier(N)             -- a barrier for N parties.
 *   new CyclicBarrier(N, action)     -- additionally run `action` on the LAST
 *                                       arriver's thread, AFTER all are
 *                                       waiting and BEFORE any are released.
 *                                       Perfect for "merge phase results"
 *                                       between iterations.
 *   barrier.await()                  -- park until all N have called await();
 *                                       returns the arrival index (N-1 for
 *                                       the trip-triggering thread, 0 for the
 *                                       last to be released, etc.).
 *   barrier.await(t, unit)           -- timed variant; throws TimeoutException.
 *
 * Why two await() forms? Because if ANY waiter is interrupted, times out, or
 * throws inside the barrier action, the barrier becomes BROKEN: every other
 * waiter's await() throws BrokenBarrierException. This is intentional --
 * partial barriers would deadlock subsequent rounds. To start over, call
 * barrier.reset().
 *
 * Typical use: iterative parallel algorithms (cellular automata, SOR, BSP-style
 * supersteps) where every worker processes its slice of data, then must
 * synchronize before reading neighbours' results for the next iteration. The
 * barrier ALSO provides the "phase end" hook -- a great place to swap buffers,
 * log progress, or check a stopping criterion.
 *
 * Latch vs Barrier in one line: a LATCH waits for EVENTS (countDown calls,
 * possibly by ONE thread, possibly many times); a BARRIER waits for THREADS
 * (each party calls await ONCE per phase) and is reusable.
 */

import java.util.concurrent.{BrokenBarrierException, CyclicBarrier, Executors, TimeUnit}

object Barriers {

  // -------------------------------------------------------------------------
  // Demo A -- N workers march through 3 PHASES, each phase aligned by the
  // barrier. The barrier ACTION runs once per phase (on the last arriver),
  // which is the natural place to compute / log per-phase aggregates.
  // -------------------------------------------------------------------------
  private def demoPhasedComputation(): Unit = {
    println("[A] CyclicBarrier: 4 workers, 3 phases, action runs on phase boundary")
    val N = 4
    val PHASES = 3
    val pool = Executors.newFixedThreadPool(N)

    @volatile var phaseNo = 0
    val barrier = new CyclicBarrier(3, new Runnable {
      def run(): Unit = {
        phaseNo += 1
        // This block runs once per trip, on the LAST arriving worker's thread,
        // while every other worker is still parked inside await().
        println(s"[A]   --- barrier action: end of phase $phaseNo ---")
      }
    })

    for (i <- 1 to N) pool.submit(new Runnable {
      def run(): Unit = {
        try {
          for (p <- 1 to PHASES) {
            Thread.sleep(50L * i)               // staggered arrival on purpose
            println(s"[A]   worker-$i finished phase $p, arriving at barrier")
            val arrivalIdx = barrier.await()    // park until everyone is here
            println(s"[A]   worker-$i released from phase $p (arrivalIndex=$arrivalIdx)")
          }
        } catch {
          case _: InterruptedException     => Thread.currentThread().interrupt()
          case _: BrokenBarrierException   => println(s"[A]   worker-$i saw a broken barrier")
        }
      }
    })

    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS)
  }

  // -------------------------------------------------------------------------
  // Demo B -- "broken barrier": one waiter is interrupted, so the barrier
  // trips with BrokenBarrierException for everyone else in the same phase.
  // -------------------------------------------------------------------------
  private def demoBrokenBarrier(): Unit = {
    println("[B] interrupting one waiter breaks the barrier for all the others")
    val barrier = new CyclicBarrier(3)

    val victim = new Thread(() => {
      try barrier.await()
      catch { case _: InterruptedException => println("[B]   victim was interrupted while waiting") }
    }, "victim")

    val peer = new Thread(() => {
      try barrier.await()
      catch { case _: BrokenBarrierException => println("[B]   peer saw BrokenBarrierException") }
    }, "peer")

    victim.start(); peer.start()
    Thread.sleep(100)                           // make sure both are parked
    victim.interrupt()                          // breaks the barrier
    victim.join(); peer.join()
    println(s"[B]   barrier.isBroken = ${barrier.isBroken}  (call reset() to reuse)")
  }

  // -------------------------------------------------------------------------
  // Demo C -- reset(): clears the broken state and re-arms the barrier for
  // the original party count. Use this after handling a break.
  // -------------------------------------------------------------------------
  private def demoReset(): Unit = {
    println("[C] reset() re-arms a broken (or just-finished) barrier")
    val barrier = new CyclicBarrier(2)

    // Trip it once with a tiny pair, then inspect.
    val t1 = new Thread(() => barrier.await())
    val t2 = new Thread(() => barrier.await())
    t1.start(); t2.start(); t1.join(); t2.join()
    println(s"[C]   after one trip: waiting=${barrier.getNumberWaiting}, parties=${barrier.getParties}")

    // No new parties are needed before reuse; a CyclicBarrier is automatically
    // reset on a clean trip. reset() is mainly for the broken case.
    barrier.reset()
    println(s"[C]   after reset():  waiting=${barrier.getNumberWaiting}, broken=${barrier.isBroken}")
  }

  def main(args: Array[String]): Unit = {
    demoPhasedComputation(); println()
//    demoBrokenBarrier();     println()
//    demoReset()
  }
}