import java.util.concurrent.{Semaphore, TimeUnit}

/**
 * Demonstrates:
 *  1) A binary Semaphore (1 permit) is NOT reentrant: if the same thread
 *     tries to acquire it twice without releasing, it will block (or fail to
 *     acquire with a timeout).
 *  2) Java/Scala synchronized is reentrant: the same thread may enter the same
 *     monitor multiple times (nested synchronized blocks) without blocking.
 *
 * Run the object to see printed output illustrating the difference.
 */
object BinarySemaphoreReentrantDemo {

  // A simple non-reentrant lock built on Semaphore(1)
  class NonReentrantLock {
    private val sem = new Semaphore(1)
    def lock(): Unit = sem.acquire()
    def tryLock(timeoutMs: Long): Boolean = sem.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)
    def unlock(): Unit = sem.release()
  }

  // Demo using NonReentrantLock (Semaphore(1))
  private def demoSemaphoreNonReentrant(): Unit = {
    println("--- demoSemaphoreNonReentrant ---")
    val lock = new NonReentrantLock

    def inner(): Unit = {
      println("[semaphore] inner(): attempting to re-acquire lock (will try with timeout)")
      val acquired = lock.tryLock(500)
      if (acquired) {
        try println("[semaphore] inner(): re-acquired lock (unexpected)")
        finally lock.unlock()
      } else {
        println("[semaphore] inner(): could NOT re-acquire lock (non-reentrant behavior)")
      }
    }

    // Outer acquires the lock then calls inner() on the SAME thread
    println("[semaphore] outer(): acquiring lock")
    lock.lock()
    try {
      println("[semaphore] outer(): lock held, calling inner() which will try to re-acquire")
      inner()
      println("[semaphore] outer(): inner() returned, releasing lock afterwards")
    } finally {
      lock.unlock()
    }

    // show that after release another thread can acquire
    val t = new Thread(() => {
      println("[semaphore] other thread: trying to acquire lock (should succeed)")
      lock.lock()
      try println("[semaphore] other thread: acquired lock")
      finally lock.unlock()
    })
    t.start(); t.join()
    println()
  }

  // Demo using synchronized (reentrant)
  private def demoSynchronizedReentrant(): Unit = {
    println("--- demoSynchronizedReentrant ---")

    val monitor = new AnyRef

    def inner(): Unit = monitor.synchronized {
      println("[synchronized] inner(): successfully entered nested synchronized block (reentrant)")
    }

    // Outer synchronized block on same monitor
    println("[synchronized] outer(): entering synchronized block")
    monitor.synchronized {
      println("[synchronized] outer(): lock held, calling inner() which will re-enter the same monitor")
      inner()
      // it sleep Current Thread just to show that we are still inside the outer block after inner() returns, and that we can do more work before exiting
      Thread.sleep(1000) // just to show that we are still inside the outer block after inner() returns
      println("[synchronized] outer(): inner() returned, will exit outer block")
    }

    // show that another thread can also acquire after exit
    val t = new Thread(() => monitor.synchronized {
      println("[synchronized] other thread: entered synchronized block after outer released")

    })
    t.start(); t.join()
    println()
  }

  def main(args: Array[String]): Unit = {
    println("Binary semaphore vs synchronized (reentrant) demo")
    println()
    demoSemaphoreNonReentrant()
    demoSynchronizedReentrant()
    println("Demo finished")
  }
}

