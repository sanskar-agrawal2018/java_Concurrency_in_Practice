/*
 * How Executors Keep Pending Runnables from Execution
 *
 * Purpose:
 *   This demo explains the internal queueing mechanism used by executors to
 *   hold pending tasks until a worker thread becomes available.
 *
 * Key Concepts:
 *
 *   1. INTERNAL QUEUE (BlockingQueue)
 *      - Executors maintain an internal BlockingQueue<Runnable>
 *      - When you call executor.execute(runnable), the task is added to this queue
 *      - The queue holds tasks that are waiting for a worker thread to become free
 *
 *   2. WORKER THREADS
 *      - A fixed pool of worker threads constantly poll the queue
 *      - Each worker runs: while(true) { Runnable task = queue.take(); task.run(); }
 *      - queue.take() BLOCKS (waits) if the queue is empty
 *      - queue.take() RETURNS immediately if a task is available
 *
 *   3. LIFECYCLE
 *      Task submitted → Added to queue → Worker picks it up → Runs it → Picks next from queue
 *
 *   4. REJECTION POLICY (if queue is full)
 *      - Some executors have bounded queues and rejection policies
 *      - If queue is full, tasks are rejected (throw RejectedExecutionException)
 *      - Or they can be queued synchronously, discarded, or delegated to caller thread
 *
 * Visual Flow:
 *
 *   ─────────────────────────────────────
 *   │ Task Submission (from user threads) │
 *   └──────────────────────────────────────┘
 *            ↓
 *   ┌─────────────────────────────────────┐
 *   │  execute(runnable) is called        │
 *   └─────────────────────────────────────┘
 *            ↓
 *   ┌─────────────────────────────────────┐
 *   │  queue.put(runnable)                │  ← Adds to internal BlockingQueue
 *   └─────────────────────────────────────┘
 *            ↓
 *   ╔═════════════════════════════════════╗
 *   ║  Queue State:                       ║
 *   ║  [Task1] [Task2] [Task3] ...        ║  ← Pending tasks waiting here
 *   ╚═════════════════════════════════════╝
 *            ↑        ↑        ↑
 *     Worker1 tries  Worker2 tries  Worker3 tries
 *     to pick up      to pick up     to pick up
 *     (blocked if queue empty)
 *
 *   ┌──────────────────────────────────────┐
 *   │  Worker Thread Loop:                 │
 *   │  while (true) {                      │
 *   │    Runnable task = queue.take();     │ ← BLOCKS if queue empty
 *   │    task.run();                       │
 *   │  }                                   │
 *   └──────────────────────────────────────┘
 *
 * Run this demo:
 *   sbt "chapter6/runMain ExecutorQueueingMechanism"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

object ExecutorQueueingMechanism {

  def main(args: Array[String]): Unit = {
    demoBasicQueueing()
    println("\n" + "=" * 80 + "\n")
    demoQueueFilling()
    println("\n" + "=" * 80 + "\n")
    demoWorkerThreadBehavior()
    println("\n" + "=" * 80 + "\n")
    demoRejectionPolicy()
    println("\n" + "=" * 80 + "\n")
    demoUnderTheHoodQueue()
  }

  /**
   * DEMO A: Basic Queueing Mechanism
   * Shows how tasks are queued and executed by worker threads.
   */
  private def demoBasicQueueing(): Unit = {
    println("DEMO A: Basic Queueing Mechanism")
    println("=" * 80)
    println("""
      |Setup: 2 worker threads, submit 5 tasks
      |
      |Expected behavior:
      |  - First 2 tasks run immediately (workers are free)
      |  - Next 3 tasks go into queue (workers are busy)
      |  - As workers finish, they pick up tasks from queue
      |
      |Queue state over time:
      |  T0: submit task1 → Queue: [task1] → Worker1 picks it → Running: task1
      |  T1: submit task2 → Queue: [] → Worker2 picks it → Running: task1, task2
      |  T2: submit task3 → Queue: [task3] → Worker1/2 are busy
      |  T3: submit task4 → Queue: [task3, task4]
      |  T4: submit task5 → Queue: [task3, task4, task5]
      |
      |  T5+: Worker1 finishes → picks task3 from queue
      |       Worker2 finishes → picks task4 from queue
      |       etc...
    """.stripMargin)

    val executor = Executors.newFixedThreadPool(2)
    val startTime = System.currentTimeMillis()

    for (i <- 1 to 5) {
      executor.execute(() => {
        val elapsed = System.currentTimeMillis() - startTime
        val thread = Thread.currentThread().getName
        println(f"[${elapsed}ms] [$thread] Task $i STARTED (was waiting in queue)")
        Thread.sleep(1000)  // Simulate work
        println(f"[${System.currentTimeMillis() - startTime}ms] [$thread] Task $i FINISHED")
      })
      Thread.sleep(100)  // Small delay between submissions for clarity
    }

    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    println("\nAll tasks completed!")
  }

  /**
   * DEMO B: Queue Filling
   * Shows exactly when tasks enter the queue vs. when they run.
   */
  private def demoQueueFilling(): Unit = {
    println("DEMO B: Queue Filling - Rapid Task Submissions")
    println("=" * 80)
    println("""
      |Setup: 1 worker thread, submit 5 tasks rapidly
      |
      |What happens:
      |  1. First task runs immediately on worker
      |  2. Tasks 2-5 are added to queue (blocked workers take from queue)
      |  3. You can see tasks waiting in queue before execution
    """.stripMargin)

    val executor = Executors.newFixedThreadPool(1)  // Only 1 worker!
    val startTime = System.currentTimeMillis()

    println("\n--- Submitting tasks rapidly ---")
    for (i <- 1 to 5) {
      val submitTime = System.currentTimeMillis() - startTime
      println(f"[${submitTime}ms] Submitting Task $i to queue")
      executor.execute(() => {
        val startRun = System.currentTimeMillis() - startTime
        val thread = Thread.currentThread().getName
        println(f"[${startRun}ms] [$thread] Task $i STARTED")
        Thread.sleep(300)
        val endRun = System.currentTimeMillis() - startTime
        println(f"[${endRun}ms] [$thread] Task $i FINISHED")
      })
    }

    println("\nNote: Above, you see all 5 submissions happen quickly (within a few ms)")
    println("But execution is serialized because only 1 worker thread exists.\n")

    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * DEMO C: Worker Thread Behavior
   * Demonstrates how worker threads continuously poll the queue.
   */
  private def demoWorkerThreadBehavior(): Unit = {
    println("DEMO C: Worker Thread Behavior - How Workers Poll the Queue")
    println("=" * 80)
    println("""
      |Worker threads run an infinite loop (pseudocode):
      |
      |  while (running) {
      |    try {
      |      Runnable task = queue.take();  // BLOCKS here if queue is empty
      |      task.run();                    // Execute the task
      |    } catch (InterruptedException) { break; }
      |  }
      |
      |Key: queue.take() is a BLOCKING operation:
      |  - If queue is NOT empty: take immediately returns a task (fast)
      |  - If queue IS empty: take() WAITS (thread is parked) until a task arrives
      |
      |This way, idle worker threads don't waste CPU spinning. They're truly blocked.
    """.stripMargin)

    // Create a custom executor that shows when workers block on queue.take()
    val queueSize = new AtomicInteger(0)
    val executor = Executors.newFixedThreadPool(2)
    val startTime = System.currentTimeMillis()

    println("\n--- Simulating Worker Blocking ---\n")

    // Submit task immediately
    executor.execute(() => {
      val elapsed = System.currentTimeMillis() - startTime
      println(f"[${elapsed}ms] Worker thread picked task from queue and is running")
      Thread.sleep(2000)
      println(f"[${System.currentTimeMillis() - startTime}ms] Worker thread finished. Now blocking on queue.take() waiting for next task...")
    })

    Thread.sleep(2500)  // Wait for worker to finish, then it blocks on queue.take()

    println(s"[${System.currentTimeMillis() - startTime}ms] (Main) Worker thread is now BLOCKED on queue.take(), waiting for a task")
    Thread.sleep(1000)
    println(s"[${System.currentTimeMillis() - startTime}ms] (Main) Submitting a new task while worker is blocked...")

    executor.execute(() => {
      val elapsed = System.currentTimeMillis() - startTime
      println(f"[${elapsed}ms] Worker UNBLOCKED! Picked new task from queue and running")
      Thread.sleep(500)
      println(f"[${System.currentTimeMillis() - startTime}ms] Task finished")
    })

    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    println("\n(Worker threads are now idle/waiting on shutdown)")
  }

  /**
   * DEMO D: Rejection Policy - What happens when queue is full
   */
  private def demoRejectionPolicy(): Unit = {
    println("DEMO D: Rejection Policy - What Happens with Bounded Queues")
    println("=" * 80)
    println("""
      |ThreadPoolExecutor can have a BOUNDED queue:
      |  - Default: unbounded (LinkedBlockingQueue with no max)
      |  - Custom: bounded (e.g., max 5 tasks in queue)
      |
      |When queue is FULL, rejection policies kick in:
      |  1. AbortPolicy (default) → throw RejectedExecutionException
      |  2. CallerRunsPolicy → run task on the calling thread (blocking submit)
      |  3. DiscardPolicy → silently discard the task
      |  4. DiscardOldestPolicy → remove oldest pending task, queue the new one
      |
      |Example with bounded queue:
    """.stripMargin)

    // Create a ThreadPoolExecutor with bounded queue
    val corePoolSize = 1
    val maxPoolSize = 1
    val queueCapacity = 2
    val queue = new LinkedBlockingQueue[Runnable](queueCapacity)

    val executor = new ThreadPoolExecutor(
      corePoolSize, maxPoolSize,
      60L, TimeUnit.SECONDS,
      queue,
      new ThreadPoolExecutor.AbortPolicy()  // Throw exception on rejection
    )

    println(f"\nSetup: 1 worker, queue capacity = $queueCapacity")
    println("Submitting tasks...\n")

    try {
      for (i <- 1 to 5) {
        try {
          executor.execute(() => {
            val thread = Thread.currentThread().getName
            println(f"  Task $i running on $thread")
            Thread.sleep(1000)
            println(f"  Task $i finished")
          })
          println(f"✓ Task $i submitted successfully (queue size: ${queue.size()})")
        } catch {
          case e: RejectedExecutionException =>
            println(f"✗ Task $i REJECTED (queue is full: ${queue.size()}/$queueCapacity)")
            println(f"  Reason: {e.getMessage}")
        }
      }
    } finally {
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  /**
   * DEMO E: Under the Hood - Manual Queue Implementation
   * Shows a simplified version of how ThreadPoolExecutor works internally.
   */
  private def demoUnderTheHoodQueue(): Unit = {
    println("DEMO E: Under the Hood - Simplified ThreadPoolExecutor Internals")
    println("=" * 80)
    println("""
      |Simplified pseudocode for ThreadPoolExecutor:
      |
      |  class ThreadPoolExecutor {
      |    val queue = new LinkedBlockingQueue[Runnable]()
      |    val workers = new Thread[] { ... }  // pool of worker threads
      |
      |    def execute(runnable) {
      |      queue.put(runnable)  // Add to queue (blocks if full)
      |      // Worker threads will pick it up
      |    }
      |
      |    // Each worker runs:
      |    def workerLoop() {
      |      while (running) {
      |        val task = queue.take()  // BLOCKS until task available
      |        task.run()               // Execute the task
      |      }
      |    }
      |  }
      |
      |Demonstration with custom simple executor:
    """.stripMargin)

    val simpleExecutor = new SimpleThreadPoolExecutor(poolSize = 2, queueCapacity = 100)
    val startTime = System.currentTimeMillis()

    println("\nSubmitting 6 tasks to a pool of 2 workers with queue capacity 100:\n")

    for (i <- 1 to 6) {
      simpleExecutor.execute(() => {
        val elapsed = System.currentTimeMillis() - startTime
        val thread = Thread.currentThread().getName
        println(f"[${elapsed}ms] [$thread] Task $i started")
        Thread.sleep(500)
        println(f"[${System.currentTimeMillis() - startTime}ms] [$thread] Task $i finished")
      })
      println(s"  → Task $i queued (queue depth: ${simpleExecutor.queueSize()})")
    }

    simpleExecutor.shutdown()
    println("\nSimple executor demonstration complete!")
  }
}

/**
 * A simplified ThreadPoolExecutor to show the internal mechanics.
 * NOT for production use - just for understanding.
 */
private class SimpleThreadPoolExecutor(poolSize: Int, queueCapacity: Int) {
  private val queue = new LinkedBlockingQueue[Runnable](queueCapacity)
  private val workers = ListBuffer[Thread]()
  @volatile private var running = true
  private val nextIdMT = new AtomicInteger(1)

  def queueSize(): Int = queue.size()

  // Start worker threads
  (1 to poolSize).foreach { _ =>
    val worker = new Thread(() => workerLoop(), s"SimpleWorker-${nextIdMT.getAndIncrement()}")
    worker.setDaemon(false)
    worker.start()
    workers += worker
  }

  def execute(task: Runnable): Unit = {
    if (!running) throw new RejectedExecutionException("Executor has been shut down")
    queue.put(task)  // Blocks if queue is full
  }

  def shutdown(): Unit = {
    running = false
    workers.foreach(_.join(5000))  // Wait for workers to finish
  }

  private def workerLoop(): Unit = {
    try {
      while (running) {
        val task = queue.take()  // BLOCKS here if queue is empty
        task.run()
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }
}

