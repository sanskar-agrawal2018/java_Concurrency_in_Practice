/*
 * States of a Thread  (java.lang.Thread.State)
 *
 * Every JVM thread is, at any instant, in exactly ONE of six states. You can
 * read the current state with `thread.getState`. The six states are:
 *
 *   NEW            Thread object created, but start() has NOT been called yet.
 *   RUNNABLE       Eligible to run -- either executing on a CPU right now, or
 *                  waiting only for a CPU time-slice. (The JVM does not
 *                  distinguish "running" from "ready"; both are RUNNABLE. A
 *                  thread blocked on file/network I/O also shows as RUNNABLE.)
 *   BLOCKED        Waiting to ACQUIRE A MONITOR LOCK so it can enter (or
 *                  re-enter) a `synchronized` block another thread holds.
 *   WAITING        Waiting INDEFINITELY for another thread to act. Entered via
 *                  Object.wait(), Thread.join(), or LockSupport.park().
 *   TIMED_WAITING  Like WAITING, but with a deadline. Entered via
 *                  Thread.sleep(t), wait(t), join(t), LockSupport.parkNanos.
 *   TERMINATED     run() has finished (normally or via an exception).
 *
 * Typical lifecycle:
 *
 *        start()                   scheduler picks it
 *   NEW --------> RUNNABLE <==============================> (on a CPU)
 *                   |  ^   ^   ^
 *      lock taken   |  |   |   |  notify() / target ends / timeout / sleep ends
 *      by another   v  |   |   |
 *               BLOCKED |   |   |
 *                       |   |   |
 *      wait()/join() ---+   |   +--- sleep(t)/wait(t)/join(t)
 *               WAITING      TIMED_WAITING
 *                   \              /
 *                    \            /   run() returns
 *                     +--> RUNNABLE -------------> TERMINATED
 *
 * A thread can never go back to NEW, and never leaves TERMINATED.
 */

object ThreadStates {

  /** Prints a labelled snapshot of a thread's current state. */
  private def show(label: String, t: Thread): Unit =
    println(f"  $label%-22s getState = ${t.getState}")

  // -------------------------------------------------------------------------
  // NEW -- constructed but never started. It has no call stack yet.
  // -------------------------------------------------------------------------
  private def demoNew(): Unit = {
    println("[NEW] a Thread that has not been start()-ed")
    val t = new Thread(() => ())
    show("before start()", t)            // NEW
  }

  // -------------------------------------------------------------------------
  // RUNNABLE -- busy doing CPU work. We watch it from the main thread.
  // -------------------------------------------------------------------------
  private def demoRunnable(): Unit = {
    println("[RUNNABLE] a thread actively burning CPU in a loop")
    val t = new Thread(() => {
      // Spin for ~300ms with no blocking call -- stays RUNNABLE the whole time.
      val deadline = System.currentTimeMillis() + 300
      var x = 0L
      while (System.currentTimeMillis() < deadline) x += 1
      println(s"[RUNNABLE][ThreadState:${Thread.currentThread().getState}] thread is running, x = $x")    })
    t.start()
    Thread.sleep(100)                    // observe it mid-loop
    show("while looping", t)             // RUNNABLE
    t.join()
  }

  // -------------------------------------------------------------------------
  // TIMED_WAITING -- parked with a deadline (here, inside Thread.sleep).
  // -------------------------------------------------------------------------
  private def demoTimedWaiting(): Unit = {
    println("[TIMED_WAITING] a thread inside Thread.sleep(...)")
    val t = new Thread(() => {
      try Thread.sleep(2000)
      catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    })
    t.start()
    Thread.sleep(100)
    show("during sleep(2000)", t)         // TIMED_WAITING
    t.interrupt()                         // wake it so the demo ends promptly
    t.join()
  }

  // -------------------------------------------------------------------------
  // WAITING -- parked with NO deadline, inside Object.wait(). It can only
  // resume when another thread calls notify()/notifyAll() on the same lock.
  // -------------------------------------------------------------------------
  private def demoWaiting(): Unit = {
    println("[WAITING] a thread inside lock.wait() (no timeout)")
    val lock = new AnyRef
    val t = new Thread(() => {
      lock.synchronized {
        try lock.wait()                   // releases the lock and parks
        catch { case _: InterruptedException => Thread.currentThread().interrupt() }
      }
    })
    t.start()
    Thread.sleep(100)
    show("during lock.wait()", t)         // WAITING
    lock.synchronized(lock.notify())      // wake it up
    t.join()
  }

  // -------------------------------------------------------------------------
  // BLOCKED -- waiting to ACQUIRE a monitor. `holder` grabs the lock and
  // sleeps inside it; `contender` then tries to enter the SAME synchronized
  // block and gets stuck at the door until the lock is free.
  // -------------------------------------------------------------------------
  private def demoBlocked(): Unit = {
    println("[BLOCKED] a thread waiting to enter a synchronized block")
    val lock = new AnyRef

    val holder = new Thread(() => lock.synchronized {
      Thread.sleep(500)                   // hold the monitor for a while
    })
    val contender = new Thread(() => lock.synchronized {
      println("Contender Entered the synchronized block") // we never reach this until `holder` releases the lock

    })

    holder.start()
    Thread.sleep(100)                     // make sure `holder` owns the lock
    contender.start()
    Thread.sleep(100)                     // give `contender` time to hit the door
    show("contender at the lock", contender) // BLOCKED
    show("holder owning the lock", holder)    // TIMED_WAITING (sleeping inside)

    holder.join()
    contender.join()
  }

  // -------------------------------------------------------------------------
  // TERMINATED -- run() has returned. The thread object still exists but can
  // never run again.
  // -------------------------------------------------------------------------
  private def demoTerminated(): Unit = {
    println("[TERMINATED] a thread whose run() has finished")
    val t = new Thread(() => ())          // trivial body -- finishes instantly
    t.start()
    t.join()                              // wait for run() to return
    show("after join()", t)               // TERMINATED
  }

  def main(args: Array[String]): Unit = {
//    demoNew();           println()
//    demoRunnable();      println()
    demoTimedWaiting();  println()
    demoWaiting();       println()
    demoBlocked();       println()
//    demoTerminated()
  }
}