/*
 * Semaphores  (java.util.concurrent.Semaphore)
 *
 * A SEMAPHORE is a counter of PERMITS. Threads acquire() permits to enter a
 * protected region and release() them to leave. If no permit is available,
 * acquire() blocks until one is released.
 *
 *   new Semaphore(N)          -- create with N permits.
 *   new Semaphore(N, true)    -- the "fair" variant: acquirers are served in
 *                                FIFO order. Slower; usually unnecessary.
 *   sem.acquire()             -- block until one permit is available; take it.
 *   sem.acquire(k)            -- atomically take k permits.
 *   sem.tryAcquire()          -- non-blocking; returns true if a permit was
 *                                taken, false otherwise.
 *   sem.tryAcquire(t, unit)   -- block up to t, returning false on timeout.
 *   sem.release() / release(k)-- return permit(s). RELEASE NEED NOT BE CALLED
 *                                BY THE THREAD THAT ACQUIRED -- a Semaphore is
 *                                NOT a lock; it has no notion of "owner".
 *
 * Two textbook uses:
 *
 *   1. THROTTLE / BOUND CONCURRENCY -- "at most K threads in this region at
 *      once" (e.g. a connection pool of size K, or a rate-limit on outbound
 *      HTTP). Create Semaphore(K); each user acquire()s before entering and
 *      release()s after.
 *
 *   2. SIGNALING / HAND-OFF -- Semaphore(0) starts with no permits, so any
 *      acquire() blocks. Another thread later release()s, "handing off" the
 *      signal. This is the building block for many other synchronizers.
 *
 * A Semaphore is NOT a lock:
 *   - permits are interchangeable; no concept of "the holder";
 *   - any thread can release a permit it never acquired (that's the point of
 *     hand-off, but it also means there is no re-entrance and no IllegalMonitor
 *     -StateException to catch you out);
 *   - more permits can be released than were acquired -- you can OVER-RELEASE
 *     by mistake, raising availablePermits() above the initial N. The class
 *     does not prevent this. Use try/finally to release exactly once.
 */

import java.util.concurrent.{Executors, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object Semaphores {

  // -------------------------------------------------------------------------
  // Demo A -- THROTTLE: at most 3 threads inside the "DB" region at any time,
  // even though 8 workers are racing to enter it.
  // -------------------------------------------------------------------------
  private def demoThrottle(): Unit = {
    println("[A] Semaphore as a throttle: at most 3 concurrent users")
    val sem = new Semaphore(3)
    val inside = new AtomicInteger(0)
    val peak = new AtomicInteger(0)
    val pool = Executors.newFixedThreadPool(8)

    for (i <- 1 to 8) pool.submit(new Runnable {
      def run(): Unit = {
        sem.acquire()
        try {
          val now = inside.incrementAndGet()
          peak.updateAndGet(p => math.max(p, now))
          println(s"[A]   worker-$i inside; concurrent=$now")
          Thread.sleep(80)
          inside.decrementAndGet()
        } finally {
          sem.release()                         // ALWAYS release in finally
        }
      }
    })

    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.SECONDS)
    println(s"[A]   peak concurrency observed = ${peak.get} (must be <= 3)")
  }

  // -------------------------------------------------------------------------
  // Demo B -- SIGNALING: Semaphore(0) used as a one-shot hand-off between
  // a producer and a consumer thread. Note that the "releasing" thread never
  // acquired the permit -- a Semaphore is not a lock.
  // -------------------------------------------------------------------------
  private def demoSignaling(): Unit = {
    println("[B] Semaphore(0) for thread-to-thread hand-off")
    val signal = new Semaphore(0)               // starts empty -- any acquire blocks

    val consumer = new Thread(() => {
      println("[B]   consumer waiting for signal...")
      signal.acquire()                          // blocks until producer releases
      println("[B]   consumer got the signal, proceeding")
    })
    val producer = new Thread(() => {
      Thread.sleep(150)
      println("[B]   producer releasing the permit (= sending the signal)")
      signal.release()                          // never acquired anything; legal
    })

    consumer.start(); producer.start()
    consumer.join();  producer.join()
  }

  // -------------------------------------------------------------------------
  // Demo C -- tryAcquire returns immediately; tryAcquire(t,unit) blocks up to
  // t and returns false on timeout. Useful when "skip this work" is better
  // than "wait forever".
  // -------------------------------------------------------------------------
  private def demoTryAcquire(): Unit = {
    println("[C] tryAcquire is the non-blocking variant; with a timeout it bounds the wait")
    val sem = new Semaphore(1)
    sem.acquire()                               // permit is now taken

    val gotNow = sem.tryAcquire()
    println(s"[C]   immediate tryAcquire -> $gotNow (false: none left)")

    val started = System.currentTimeMillis()
    val gotTimed = sem.tryAcquire(120, TimeUnit.MILLISECONDS)
    val elapsed = System.currentTimeMillis() - started
    println(s"[C]   tryAcquire(120ms) -> $gotTimed after ~${elapsed}ms")

    sem.release()
  }

  // -------------------------------------------------------------------------
  // Demo D -- a Semaphore is NOT a lock. You can over-release and bump the
  // permit count above the initial N. The class will not warn you.
  // -------------------------------------------------------------------------
  private def demoOverRelease(): Unit = {
    println("[D] permit counting: Semaphore is a counter, not an owned mutex")
    val initialPermits = 2
    val sem = new Semaphore(initialPermits)

    def printCount(step: String): Unit = {
      val available = sem.availablePermits()
      val acquiredFromInitial = initialPermits - available
      println(
        f"[D]   $step%-30s available=$available%d, acquired-from-initial=$acquiredFromInitial%d"
      )
    }

    printCount("created")

    sem.acquire()
    printCount("after 1 acquire()")

    sem.acquire()
    printCount("after 2 acquire() calls")

    sem.release()
    printCount("after 1 release()")

    sem.release()
    printCount("after matching releases")

    sem.release()
    printCount("after extra release()")

    sem.release(2)
    printCount("after release(2)")
    println("[D]   available permits are now above the initial 2; unlike a mutex, Semaphore allows this")
  }

  def main(args: Array[String]): Unit = {
    demoThrottle();   println()
    demoSignaling();  println()
    demoTryAcquire(); println()
    demoOverRelease()
  }
}
