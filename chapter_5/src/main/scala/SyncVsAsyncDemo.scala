/*
 * Synchronous vs Asynchronous Execution Demo
 *
 * This example demonstrates the critical differences between synchronous (blocking)
 * and asynchronous (non-blocking) execution patterns in concurrent programming.
 *
 * KEY CONCEPTS:
 * =============
 *
 * SYNCHRONOUS (applySync / blocking):
 *   - Caller BLOCKS until the operation completes
 *   - Result is returned directly
 *   - Thread is occupied (cannot do other work)
 *   - Simpler to reason about (straightforward control flow)
 *   - PROBLEM: If many operations stack, all threads get blocked → Thread pool exhaustion
 *
 * ASYNCHRONOUS (applyAsync / non-blocking):
 *   - Caller does NOT block; returns immediately with a Future
 *   - Operation runs on a separate thread/executor
 *   - Original thread can continue to other work
 *   - Better resource utilization (fewer threads wasted)
 *   - CHALLENGE: Must compose/chain Futures; control flow is less linear
 *
 * ANALOGY:
 * ========
 *   Sync:  You call a restaurant and wait on the line until your order is taken.
 *   Async: You text the restaurant and they reply when ready. You do other things meanwhile.
 *
 * WHEN TO USE:
 * ============
 *   Sync:  CPU-bound work, legacy code, or small operations where simplicity outweighs overhead.
 *   Async: I/O-bound (network, disk), high concurrency scenarios, or must scale to many operations.
 */

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object SyncVsAsyncDemo {

  // =========================================================================
  // HELPER: Simulated expensive I/O operation (e.g., HTTP request, DB query)
  // =========================================================================
  private def simulateIOOperation(taskId: Int, durationMs: Long): String = {
    val threadName = Thread.currentThread().getName
    println(s"    [IO-Task-$taskId] Starting on thread: $threadName")
    Thread.sleep(durationMs)  // simulate I/O latency
    val result = s"Result-$taskId (completed after ${durationMs}ms on $threadName)"
    println(s"    [IO-Task-$taskId] Finished: $result")
    result
  }

  // =========================================================================
  // DEMO A: SYNCHRONOUS (BLOCKING) EXECUTION
  // =========================================================================
  private def demoSyncronousExecution(): Unit = {
    println("=" * 80)
    println("DEMO A: SYNCHRONOUS (BLOCKING) EXECUTION")
    println("=" * 80)
    println("""
      Main thread calls simulateIOOperation() directly.
      Each call BLOCKS the main thread until completion.
      The thread is stuck doing nothing while waiting for I/O.
    """.stripMargin)

    val startTime = System.currentTimeMillis()
    val mainThread = Thread.currentThread().getName

    println(s"\n[Main] Starting on thread: $mainThread")

    // Task 1: 300ms (blocks main thread)
    val result1 = simulateIOOperation(1, 300)
    println(s"[Main] Got: $result1")

    // Task 2: 300ms (blocks main thread)
    val result2 = simulateIOOperation(2, 300)
    println(s"[Main] Got: $result2")

    // Task 3: 300ms (blocks main thread)
    val result3 = simulateIOOperation(3, 300)
    println(s"[Main] Got: $result3")

    val elapsed = System.currentTimeMillis() - startTime
    println(f"\n[Main] Total time: ${elapsed}ms (≈ 900ms SEQUENTIAL)")
    println("[Main] ✗ Thread was blocked the entire time; no parallelism possible")
    println()
  }

  // =========================================================================
  // DEMO B: ASYNCHRONOUS (NON-BLOCKING) EXECUTION
  // =========================================================================
  private def demoAsynchronousExecution(): Unit = {
    println("=" * 80)
    println("DEMO B: ASYNCHRONOUS (NON-BLOCKING) EXECUTION")
    println("=" * 80)
    println("""
      Main thread launches Future tasks; each returns immediately.
      Operations run on a separate executor thread pool.
      Main thread continues executing other work without blocking.
      Results are collected later via Future.map, flatMap, or Await.
    """.stripMargin)

    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

    val startTime = System.currentTimeMillis()
    val mainThread = Thread.currentThread().getName

    println(s"\n[Main] Starting on thread: $mainThread")

    // Launch 3 tasks CONCURRENTLY (returns immediately with Futures)
    println("[Main] Launching 3 async tasks...")
    val future1 = Future { simulateIOOperation(1, 300) }
    val future2 = Future { simulateIOOperation(2, 300) }
    val future3 = Future { simulateIOOperation(3, 300) }
    println("[Main] All tasks launched! Main thread continues...")

    // Can do other work here while tasks run in parallel
    println("[Main] Doing other work while tasks run in parallel...")
    Thread.sleep(100)
    println("[Main] Still doing other work...")

    // Now wait for all results using for-comprehension (Future composition)
    val combinedFuture = for {
      r1 <- future1
      r2 <- future2
      r3 <- future3
    } yield (r1, r2, r3)

    // Block only at the end to collect results
    val (result1, result2, result3) = Await.result(combinedFuture, 10.seconds)
    println(s"[Main] Got results: $result1, $result2, $result3")

    val elapsed = System.currentTimeMillis() - startTime
    println(f"\n[Main] Total time: ${elapsed}ms (≈ 300ms PARALLEL, not 900ms!)")
    println("[Main] ✓ Main thread stayed responsive; 3 tasks ran in parallel")
    println()
  }

  // =========================================================================
  // DEMO C: COMPARISON - Side-by-side with Thread Monitoring
  // =========================================================================
  private def demoComparison(): Unit = {
    println("=" * 80)
    println("DEMO C: RESOURCE UTILIZATION COMPARISON")
    println("=" * 80)

    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

    val threadPoolSize = 5
    val numTasks = 10
    val operationDurationMs = 100L

    println(f"\nSetup: ${threadPoolSize} worker threads, ${numTasks} tasks, ${operationDurationMs}ms each\n")

    // -----------------------------------------------------------------------
    // Scenario 1: Synchronous (blocking) -- will exhaust threads
    // -----------------------------------------------------------------------
    println("\n--- Scenario 1: SYNC (Blocking) ---")
    println(s"Submitting ${numTasks} sync tasks to pool with ${threadPoolSize} threads...")

    val syncStartTime = System.currentTimeMillis()
    val syncPool = Executors.newFixedThreadPool(threadPoolSize)
    val activeThreadsSync = new AtomicInteger(0)
    val maxActiveSync = new AtomicInteger(0)

    for (i <- 0 until numTasks) {
      syncPool.submit(new Runnable {
        def run(): Unit = {
          val active = activeThreadsSync.incrementAndGet()
          maxActiveSync.updateAndGet(m => Math.max(m, active))
          println(f"  [Sync-$i%2d] Active workers: $active")
          Thread.sleep(operationDurationMs)
          activeThreadsSync.decrementAndGet()
        }
      })
    }
    syncPool.shutdown()
    syncPool.awaitTermination(30, TimeUnit.SECONDS)
    val syncElapsed = System.currentTimeMillis() - syncStartTime

    println(f"  → Total time: ${syncElapsed}ms")
    println(f"  → Max active threads: ${maxActiveSync.get}/$threadPoolSize")
    println(f"  → Timeline: Tasks run sequentially (or nearly so) due to blocking")

    // -----------------------------------------------------------------------
    // Scenario 2: Asynchronous (non-blocking) -- efficient parallelism
    // -----------------------------------------------------------------------
    println("\n--- Scenario 2: ASYNC (Non-blocking) ---")
    println(s"Submitting ${numTasks} async tasks to pool with ${threadPoolSize} threads...")

    val asyncStartTime = System.currentTimeMillis()
    val activeThreadsAsync = new AtomicInteger(0)
    val maxActiveAsync = new AtomicInteger(0)

    val futures = for (i <- 0 until numTasks) yield {
      Future {
        val active = activeThreadsAsync.incrementAndGet()
        maxActiveAsync.updateAndGet(m => Math.max(m, active))
        println(f"  [Async-$i%2d] Active workers: $active")
        Thread.sleep(operationDurationMs)
        activeThreadsAsync.decrementAndGet()
        s"Result-$i"
      }
    }

    val allAsync = Future.sequence(futures)
    Await.ready(allAsync, 30.seconds)
    val asyncElapsed = System.currentTimeMillis() - asyncStartTime

    println(f"  → Total time: ${asyncElapsed}ms")
    println(f"  → Max active threads: ${maxActiveAsync.get}/$threadPoolSize")
    println(f"  → Timeline: Tasks run in parallel; threads reused efficiently")

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    println("\n" + "=" * 80)
    println("SUMMARY:")
    println(f"  Sync time:  ${syncElapsed}ms  (max ${maxActiveSync.get} active threads)")
    println(f"  Async time: ${asyncElapsed}ms  (max ${maxActiveAsync.get} active threads)")
    println(f"  Speedup:    ${(syncElapsed.toDouble / asyncElapsed).toFloat}x faster with async!")
    println("=" * 80)
    println()
  }

  // =========================================================================
  // DEMO D: EXCEPTION HANDLING IN ASYNC
  // =========================================================================
  private def demoAsyncExceptionHandling(): Unit = {
    println("=" * 80)
    println("DEMO D: EXCEPTION HANDLING IN ASYNC")
    println("=" * 80)

    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

    println("\nAsync operations wrap exceptions in Failure; must be handled.\n")

    // Task that succeeds
    val successFuture = Future {
      Thread.sleep(100)
      "Success!"
    }

    // Task that fails
    val failureFuture = Future {
      Thread.sleep(100)
      throw new RuntimeException("Simulated I/O error")
    }

    // Task that recovers
    val recoveryFuture = failureFuture.recover {
      case ex: RuntimeException => s"Recovered from error: ${ex.getMessage}"
    }

    println("[Async-Success]")
    val r1 = Await.result(successFuture, 1.second)
    println(s"  → $r1\n")

    println("[Async-Failure (caught)]")
    val r2 = Await.result(failureFuture.fallbackTo(Future("Fallback result")), 1.second)
    println(s"  → $r2\n")

    println("[Async-Recovery (recover)]")
    val r3 = Await.result(recoveryFuture, 1.second)
    println(s"  → $r3\n")

    println("✓ Async allows composable error handling via recover/fallbackTo")
    println()
  }

  // =========================================================================
  // DEMO E: APPLYING WITH TRANSFORMATION (map vs flatMap)
  // =========================================================================
  private def demoFutureTransformations(): Unit = {
    println("=" * 80)
    println("DEMO E: FUTURE TRANSFORMATIONS (map vs flatMap)")
    println("=" * 80)

    implicit val executionContext: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

    println("""
      map:     Takes a value and returns a new value (wraps in Future automatically)
      flatMap: Takes a value and returns a FUTURE (flattens nested Futures)
    """.stripMargin)

    println("\n--- Using map (value transformation) ---")
    val future1 = Future { 10 }
    val mapped = future1.map { x => x * 2 }  // 10 → 20
    println(s"  10 * 2 = ${Await.result(mapped, 1.second)}")

    println("\n--- Using flatMap (chaining Futures) ---")
    val future2 = Future { 5 }
    val flatMapped = future2.flatMap { x =>
      Future {
        Thread.sleep(100)
        x + 15  // 5 + 15 = 20
      }
    }
    println(s"  5 + 15 (via flatMap) = ${Await.result(flatMapped, 1.second)}")

    println("\n✓ flatMap chains async operations without nested Future[Future[T]]")
    println()
  }

  def main(args: Array[String]): Unit = {
    demoSyncronousExecution()
    demoAsynchronousExecution()
    demoComparison()
    demoAsyncExceptionHandling()
    demoFutureTransformations()

    println("=" * 80)
    println("KEY TAKEAWAYS:")
    println("=" * 80)
    println("""
      1. SYNC is simpler but BLOCKS threads → poor for I/O-bound workloads
      2. ASYNC is responsive and allows parallelism → better for scalability
      3. ASYNC requires composing Futures (map, flatMap, for-comprehension)
      4. Use SYNC only for CPU-intensive or short operations
      5. Use ASYNC for network, disk, or any potentially slow operations
      6. Thread pools are precious; don't block threads unnecessarily!
    """.stripMargin)
    println("=" * 80)
  }
}

