/**
 * Chapter 7 -- Child task exception propagation, built from scratch.
 *
 * Problem: when a child Thread throws an uncaught exception, the JVM
 * simply prints a stack trace and the parent thread has no idea it happened.
 * try/catch around Thread.start() or Thread.join() does NOT catch exceptions
 * thrown inside the child.
 *
 * This file shows three progressive implementations, all hand-rolled:
 *
 *   STEP 1 -- naive: prove that parent try/catch does NOT catch child exceptions.
 *
 *   STEP 2 -- from scratch: build TaskHandle<T>, a minimal Future-like
 *             container using only a Thread, CountDownLatch, and volatile
 *             fields.  Parent calls handle.get() which blocks until the child
 *             finishes, then either returns the result or re-throws the
 *             child's exception wrapped in ChildTaskException.
 *
 *   STEP 3 -- chain: Parent -> Child -> GrandChild.  GrandChild throws;
 *             the exception propagates up through Child's TaskHandle and then
 *             through Parent's TaskHandle so the top-level main() catches it.
 *
 * Run:
 *   sbt "chapter7/runMain ChildTaskExceptionDemo"
 */

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

object ChildTaskExceptionDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("STEP 1 -- naive: parent try/catch does NOT reach child exception")
    println("=" * 70)
    step1Naive()

    println()
    println("=" * 70)
    println("STEP 2 -- from scratch: TaskHandle lets parent catch child exception")
    println("=" * 70)
    step2TaskHandle()

    println()
    println("=" * 70)
    println("STEP 3 -- chain: GrandChild throws -> Child propagates -> Parent catches")
    println("=" * 70)
    step3Chain()
  }

  // ---------------------------------------------------------------------------
  // STEP 1 -- naive
  // ---------------------------------------------------------------------------
  private def step1Naive(): Unit = {
    println("[main] starting child thread inside try/catch -- will the parent catch?")

    try {
      val child = new Thread(() => {
        println(s"[${Thread.currentThread().getName}] child about to throw RuntimeException")
        throw new RuntimeException("CHILD BLEW UP")
      }, "naive-child")

      child.start()
      // join() re-throws nothing -- the child's exception is already gone
      child.join()
      println("[main] join() returned normally -- parent did NOT see the child exception!")
    } catch {
      case ex: Exception =>
        // This block is NEVER reached for child thread exceptions.
        println(s"[main] caught (this line should NOT print): $ex")
    }

    println("[main] STEP 1 conclusion: child exception is silently lost unless we build a bridge")
  }

  // ---------------------------------------------------------------------------
  // STEP 2 -- TaskHandle
  // ---------------------------------------------------------------------------
  private def step2TaskHandle(): Unit = {

    // ---- success case ----
    println("[main] launching child that returns a result")
    val successHandle = TaskHandle.run("child-ok") { () =>
      println(s"[${Thread.currentThread().getName}] doing work...")
      Thread.sleep(200)
      42
    }

    try {
      val result = successHandle.get()
      println(s"[main] child finished -- result=$result")
    } catch {
      case ex: ChildTaskException =>
        println(s"[main] unexpected failure: ${ex.getMessage}")
    }

    println()

    // ---- failure case ----
    println("[main] launching child that throws")
    val failHandle = TaskHandle.run("child-fail") { () =>
      println(s"[${Thread.currentThread().getName}] about to throw...")
      Thread.sleep(100)
      throw new IllegalStateException("child computation failed")
      0 // unreachable, needed for type inference
    }

    try {
      val _ = failHandle.get()
      println("[main] should not reach here")
    } catch {
      case ex: ChildTaskException =>
        println(s"[main] caught ChildTaskException: ${ex.getMessage}")
        println(s"[main] original cause         : ${ex.getCause.getClass.getSimpleName}: ${ex.getCause.getMessage}")
    }
  }

  // ---------------------------------------------------------------------------
  // STEP 3 -- chain: parent -> child -> grandchild
  // ---------------------------------------------------------------------------
  private def step3Chain(): Unit = {
    // Parent spawns Child; Child spawns GrandChild; GrandChild throws.
    // Each layer uses TaskHandle so the exception travels all the way up.
    val parentHandle = TaskHandle.run("parent") { () =>
      println(s"[${Thread.currentThread().getName}] parent started; spawning child")

      val childHandle = TaskHandle.run("child") { () =>
        println(s"[${Thread.currentThread().getName}] child started; spawning grandchild")

        val grandchildHandle = TaskHandle.run("grandchild") { () =>
          println(s"[${Thread.currentThread().getName}] grandchild about to throw!")
          Thread.sleep(100)
          throw new ArithmeticException("divide by zero in grandchild")
          0
        }

        // Child calls get() -- if grandchild threw, this re-throws as ChildTaskException.
        try {
          grandchildHandle.get()
        } catch {
          case ex: ChildTaskException =>
            // Child knows it cannot recover -- wrap and re-throw so parent sees it.
            println(s"[${Thread.currentThread().getName}] child caught grandchild failure; re-throwing")
            throw new RuntimeException("child: grandchild failed", ex)
        }
      }

      // Parent calls get() on child -- receives the wrapped exception.
      childHandle.get()
      "done" // unreachable
    }

    try {
      parentHandle.get()
    } catch {
      case ex: ChildTaskException =>
        println(s"[main] parent caught the chain exception: ${ex.getMessage}")
        println(s"[main] cause (child level)       : ${ex.getCause.getMessage}")
        println(s"[main] cause (grandchild level)  : ${ex.getCause.getCause.getCause.getMessage}")
    }

    println("[main] STEP 3 complete -- full exception chain traveled from grandchild to main")
  }
}

// =============================================================================
// ChildTaskException -- wraps the original exception thrown in a child thread.
// =============================================================================
final class ChildTaskException(message: String, cause: Throwable)
    extends Exception(message, cause)

// =============================================================================
// TaskHandle[T] -- minimal Future built from scratch.
//
// Internals:
//   - a plain Thread that runs the user's Callable
//   - CountDownLatch(1): child counts down when done (success OR failure)
//   - AtomicReference[Option[T]]: holds the result on success
//   - AtomicReference[Option[Throwable]]: holds the exception on failure
//
// The parent calls get() which:
//   1. blocks on the latch until the child is done
//   2. inspects the exception slot first; if set, wraps and re-throws
//   3. otherwise returns the value from the result slot
// =============================================================================
final class TaskHandle[T] private (childThread: Thread) {

  private val latch     = new CountDownLatch(1)
  private val resultRef = new AtomicReference[Option[T]](None)
  private val errorRef  = new AtomicReference[Option[Throwable]](None)

  private[TaskHandle] def setResult(v: T): Unit = {
    resultRef.set(Some(v))
    latch.countDown()
  }

  private[TaskHandle] def setError(ex: Throwable): Unit = {
    errorRef.set(Some(ex))
    latch.countDown()
  }

  /**
   * Blocks until the child thread finishes, then either returns the result
   * or re-throws the child's exception as a ChildTaskException so the
   * parent's try/catch can handle it normally.
   */
  def get(): T = {
    latch.await() // block until child calls countDown()

    errorRef.get() match {
      case Some(ex) =>
        val name = childThread.getName
        println(s"[${Thread.currentThread().getName}] TaskHandle.get(): child '$name' failed -- wrapping and re-throwing")
        throw new ChildTaskException(
          s"child task '${childThread.getName}' threw ${ex.getClass.getSimpleName}",
          ex
        )
      case None =>
        resultRef.get().get // safe: latch ensures one of the two slots is set
    }
  }
}

object TaskHandle {
  /**
   * Runs `body` in a new daemon thread named `name`.
   * The returned TaskHandle lets the parent block for the result and catch
   * any exception the child throws -- all without ExecutorService or Future.
   */
  def run[T](name: String)(body: () => T): TaskHandle[T] = {
    val handle = new TaskHandle[T](new Thread(name)) // placeholder; real thread below

    val thread = new Thread(() => {
      println(s"[${Thread.currentThread().getName}] thread started")
      try {
        val result = body()
        handle.setResult(result)
        println(s"[${Thread.currentThread().getName}] thread finished with result=$result")
      } catch {
        case ex: Throwable =>
          handle.setError(ex)
          println(s"[${Thread.currentThread().getName}] thread finished with error: ${ex.getMessage}")
      }
    }, name)

    thread.setDaemon(true)
    thread.start()
    handle
  }
}
