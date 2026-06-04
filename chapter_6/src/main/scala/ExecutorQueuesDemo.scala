/**
 * Chapter 6 - Executor Queues Demo
 *
 * This demo shows different queue types for ThreadPoolExecutor, how they behave,
 * and when to use each one.
 *
 * The queue passed to ThreadPoolExecutor controls:
 *   - How many tasks can wait in the queue (bounded vs unbounded)
 *   - FIFO vs priority order
 *   - When new threads are created (queue full = create thread up to max size)
 *   - When tasks are rejected (pool saturated = rejection policy)
 *
 * Run:
 *   sbt "chapter6/runMain ExecutorQueuesDemo"
 */

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

object ExecutorQueuesDemo {

  def main(args: Array[String]): Unit = {
    demoExecutorQueues()
  }

  /**
   * DEMO: Different queue types for ThreadPoolExecutor — behavior, trade-offs, and when to use each
   *
   * This demo shows 4 common queue types and their impact.
   */
  private def demoExecutorQueues(): Unit = {
    section(1, "ArrayBlockingQueue (Bounded FIFO)")
    demoArrayBlockingQueue()
    println()
    section(2, "LinkedBlockingQueue (Unbounded FIFO)")
    demoLinkedBlockingQueue()
    println()
    section(3, "SynchronousQueue (Zero capacity, direct handoff)")
    demoSynchronousQueue()
    println()
    section(4, "PriorityBlockingQueue (Unbounded priority-ordered)")
    demoPriorityBlockingQueue()
  }

  /**
   * ArrayBlockingQueue: A bounded FIFO queue with fixed capacity.
   *
   * Characteristics:
   *   - BOUNDED: capacity specified at construction, never grows
   *   - FIFO: tasks run in the order they were submitted
   *   - FAIR: optional fairness flag for thread scheduling
   *   - When queue is FULL: further submissions create new threads (up to maxPoolSize),
   *     then if thread pool is also full, rejection policy kicks in
   *
   * Use when:
   *   - You want to limit memory usage (bounded queue = predictable memory)
   *   - You want back-pressure (rejection when overloaded rather than unbounded growth)
   *   - You have a known upper bound on concurrent tasks
   *   - Example: web request handler with bounded task queue
   *
   * Avoid when:
   *   - You need priority-based scheduling
   *   - You need unbounded growth under spiky load
   */
  private def demoArrayBlockingQueue(): Unit = {
    println("[ArrayBlockingQueue] Bounded FIFO queue, rejects when full")
    println("-" * 78)

    val queueCapacity = 2
    val pool = new ThreadPoolExecutor(
      1,           // core threads
      3,           // max threads
      1L,
      TimeUnit.SECONDS,
      new ArrayBlockingQueue[Runnable](queueCapacity), // BOUNDED queue
      namedFactory("arrayqueue"),
      new ThreadPoolExecutor.AbortPolicy()        // reject when full
    )

    try {
      // Submit 6 tasks to a pool with 1 core, 3 max, queue of 2:
      // - Task 1: runs immediately (core thread)
      // - Task 2: queued (queue slot 1)
      // - Task 3: queued (queue slot 2)
      // - Task 4: creates 2nd thread (queue full, still below max)
      // - Task 5: runs on 2nd thread OR queued
      // - Task 6: rejects because queue full and pool at max
      val release = new CountDownLatch(1)
      for (i <- 1 to 6) {
        try {
          pool.submit(new Runnable {
            val taskId: Int = i
            override def run(): Unit = {
              try {
                println(s"  [ARRAY] task-$taskId started on ${Thread.currentThread().getName}")
                release.await() // block until released
              } catch {
                case _: InterruptedException => Thread.currentThread().interrupt()
              }
            }
          })
          println(s"  [ARRAY] task-$i submitted successfully")
        } catch {
          case _: RejectedExecutionException =>
            println(s"  [ARRAY] task-$i REJECTED (queue full + pool at max)")
        }
      }

      println(s"  [ARRAY] pool size = ${pool.getPoolSize}, queue size = ${pool.getQueue.size()}")
      release.countDown()
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * LinkedBlockingQueue: An unbounded (or optionally bounded) FIFO queue using linked nodes.
   *
   * Characteristics:
   *   - UNBOUNDED by default: capacity = Integer.MAX_VALUE, will grow until memory runs out
   *   - Can be bounded: new LinkedBlockingQueue(maxCapacity)
   *   - FIFO: tasks run in the order they were submitted
   *   - More scalable than ArrayBlockingQueue under high concurrency (uses different locks)
   *   - When queue is FULL (if bounded): same as ArrayBlockingQueue
   *
   * Use when:
   *   - You want to accept all tasks without rejection (unbounded mode)
   *   - You have bursty traffic and can afford to buffer tasks
   *   - You need FIFO ordering
   *   - Memory is not a concern or you have tuned limits elsewhere
   *   - Example: task broker, background job queue
   *
   * Avoid when:
   *   - You need to limit memory strictly (it's unbounded by default!)
   *   - You want to apply back-pressure to callers
   *   - You need priority scheduling
   *
   * Note: Executors.newFixedThreadPool() and newCachedThreadPool() use LinkedBlockingQueue by default
   */
  private def demoLinkedBlockingQueue(): Unit = {
    println("[LinkedBlockingQueue] Unbounded FIFO queue (by default)")
    println("-" * 78)

    val pool = new ThreadPoolExecutor(
      1,           // core threads
      3,           // max threads (rarely reached because queue is unbounded)
      1L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable](), // UNBOUNDED queue
      namedFactory("linkedqueue"),
      new ThreadPoolExecutor.AbortPolicy()
    )

    try {
      // Submit 10 tasks to a pool with 1 core thread, 3 max, unbounded queue:
      // - Task 1: runs immediately (core thread)
      // - Tasks 2-10: all queued (queue grows unbounded)
      // No task is ever rejected, because queue keeps growing until OOM
      val release = new CountDownLatch(1)
      for (i <- 1 to 10) {
        try {
          pool.submit(new Runnable {
            val taskId: Int = i
            override def run(): Unit = {
              try {
                println(s"  [LINKED] task-$taskId started on ${Thread.currentThread().getName}")
                release.await()
              } catch {
                case _: InterruptedException => Thread.currentThread().interrupt()
              }
            }
          })
          println(s"  [LINKED] task-$i submitted successfully")
        } catch {
          case _: RejectedExecutionException =>
            println(s"  [LINKED] task-$i REJECTED (should never happen with unbounded queue)")
        }
      }

      println(s"  [LINKED] pool size = ${pool.getPoolSize}, queue size = ${pool.getQueue.size()}")
      println(s"  [LINKED] notice: only 1 thread active despite 10 submissions (queue buffers the rest)")
      release.countDown()
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * SynchronousQueue: A queue with ZERO internal capacity (no buffering).
   *
   * Characteristics:
   *   - NO BUFFERING: each put() must be matched by a take() and vice versa
   *   - If a producer calls submit(task), it blocks until a consumer thread is free to take it
   *   - Effectively: producer and consumer hand off the task directly
   *   - When core threads are busy: new threads are created as needed (up to maxPoolSize)
   *   - Never queues; always creates threads (up to limit)
   *
   * Use when:
   *   - You MUST NOT buffer tasks (strict immediate handoff)
   *   - You want dynamic thread creation without queue buildup
   *   - You want each task execution to block the submitter if no thread is free
   *   - Example: real-time systems, low-latency trading, direct handoff scenarios
   *   - Executors.newCachedThreadPool() uses SynchronousQueue (creates unbounded threads)
   *
   * Avoid when:
   *   - You want to absorb bursty traffic without blocking the caller
   *   - You want predictable thread count (SynchronousQueue creates many threads under load)
   *   - You want memory-efficient buffering
   *
   * Behavior:
   *   - Very aggressive thread creation: will grow pool whenever a task arrives and no thread is idle
   *   - Can create many threads if maxPoolSize allows (be careful!)
   */
  private def demoSynchronousQueue(): Unit = {
    println("[SynchronousQueue] Zero-capacity queue (direct handoff, no buffering)")
    println("-" * 78)

    val pool = new ThreadPoolExecutor(
      1,             // core threads
      5,             // max threads (SynchronousQueue will create up to this)
      1L,
      TimeUnit.SECONDS,
      new SynchronousQueue[Runnable](), // ZERO capacity
      namedFactory("syncqueue"),
      new ThreadPoolExecutor.AbortPolicy()
    )

    try {
      // Submit 6 tasks to a pool with 1 core, 5 max, SynchronousQueue:
      // - Task 1: submitted by main thread, handed off to core thread
      // - Task 2-5: submitted by main thread, each creates a new thread (up to max)
      // - Task 6: no thread available and max reached, REJECTED
      val release = new CountDownLatch(1)
      for (i <- 1 to 6) {
        try {
          pool.submit(new Runnable {
            val taskId: Int = i
            override def run(): Unit = {
              try {
                println(s"  [SYNC] task-$taskId started on ${Thread.currentThread().getName}")
                release.await()
              } catch {
                case _: InterruptedException => Thread.currentThread().interrupt()
              }
            }
          })
          println(s"  [SYNC] task-$i submitted successfully")
        } catch {
          case _: RejectedExecutionException =>
            println(s"  [SYNC] task-$i REJECTED (no thread available, queue empty, limit reached)")
        }
      }

      println(s"  [SYNC] pool size = ${pool.getPoolSize}, queue size = ${pool.getQueue.size()} (always 0!)")
      println(s"  [SYNC] notice: pool grew to 5 threads (one per submission) because SynchronousQueue has no buffering")
      release.countDown()
    } finally {
      shutdownCleanly(pool)
    }
  }

  /**
   * PriorityBlockingQueue: An unbounded queue that orders tasks by priority (not FIFO).
   *
   * Characteristics:
   *   - UNBOUNDED: grows until memory runs out
   *   - PRIORITY: tasks taken in priority order (min-heap), not insertion order
   *   - Must wrap tasks with a Comparator-based wrapper (FutureTask is not Comparable)
   *   - Useful for: high-priority tasks should run before low-priority ones
   *   - When core threads are busy: tasks queue in priority order
   *
   * Use when:
   *   - You have tasks with different priorities (urgent vs background)
   *   - You want urgent tasks to jump the queue
   *   - Example: job scheduler, payment processing (high-priority payments first)
   *
   * Avoid when:
   *   - You need strict FIFO semantics
   *   - You need bounded resources (it's unbounded)
   *   - You need memory-efficient buffering
   *
   * Note: ThreadPoolExecutor wraps Runnables in FutureTask, which is not Comparable.
   *       For PriorityBlockingQueue to work, you must use a custom RunnableFuture wrapper
   *       that implements Comparable, or use a custom queue + executor.
   */
  private def demoPriorityBlockingQueue(): Unit = {
    println("[PriorityBlockingQueue] Unbounded queue ordered by priority (not FIFO)")
    println("-" * 78)

    // To use PriorityBlockingQueue with ThreadPoolExecutor, we need a custom RunnableFuture
    // that is Comparable. The standard ThreadPoolExecutor wraps tasks in FutureTask,
    // but FutureTask does not implement Comparable, so PriorityBlockingQueue setup fails.
    //
    // For simplicity in this demo, we show the concept:
    // 1. Tasks are submitted with explicit priority
    // 2. They are queued in priority order (if properly wrapped)
    // 3. Higher-priority tasks run first

    // For demonstration, we use a simpler approach: show the concept with manual management
    // Rather than fighting the ThreadPoolExecutor constraint, we describe the use case.

    val _priorityQueue = new PriorityBlockingQueue[String]()

    // Tasks with priorities (lower number = higher priority)
    val tasks = List(
      (3, "low-priority task alpha"),
      (1, "HIGH-priority task beta"),
      (2, "medium-priority task gamma")
    )

    println("  [PRIORITY] Enqueueing tasks with priorities (lower = higher priority)::")
    for ((priority, name) <- tasks) {
      // In a real scenario, you'd wrap these with a Comparable wrapper.
      // For demo, just show the order in which they'd be executed.
      println(s"  [PRIORITY] priority=$priority: $name")
    }

    // Show priority-order execution
    val sorted = tasks.sortBy(_._1)  // sort by priority (lower first = higher priority)
    println()
    println("  [PRIORITY] Execution order (if using a priority-aware executor):")
    for ((priority, name) <- sorted) {
      println(s"  [PRIORITY] priority=$priority: $name (would run now)")
    }

    println()
    println("  [PRIORITY] Key insight: PriorityBlockingQueue requires Comparable tasks,")
    println("  [PRIORITY] but ThreadPoolExecutor wraps them in non-Comparable FutureTask.")
    println("  [PRIORITY] Solution: Use a custom ThreadPoolExecutor with custom RunnableFuture implementation.")
  }

  /**
   * Summary: Which queue to use?
   * ┌──────────────────────────────────────────────────────────────────────┐
   * │ Queue Type              │ Capacity    │ Order  │ Best For            │
   * ├──────────────────────────────────────────────────────────────────────┤
   * │ ArrayBlockingQueue      │ BOUNDED     │ FIFO   │ Back-pressure, mem   │
   * │ (fixed size)            │             │        │ bounded              │
   * ├──────────────────────────────────────────────────────────────────────┤
   * │ LinkedBlockingQueue     │ UNBOUNDED   │ FIFO   │ Bursty traffic,      │
   * │ (default for Fixed/Cache)                       │ no rejections        │
   * ├──────────────────────────────────────────────────────────────────────┤
   * │ SynchronousQueue        │ ZERO        │ N/A    │ Direct handoff,      │
   * │ (hand-off, no buffering)│             │        │ dynamic thread       │
   * │                         │             │        │ creation             │
   * ├──────────────────────────────────────────────────────────────────────┤
   * │ PriorityBlockingQueue   │ UNBOUNDED   │ PRTY   │ Priority-based       │
   * │ (heap, Comparable)      │             │        │ scheduling           │
   * └──────────────────────────────────────────────────────────────────────┘
   *
   * Rule of thumb:
   *   - Strict memory budget? Use ArrayBlockingQueue (bounded)
   *   - Accept all tasks, soak up spikes? Use LinkedBlockingQueue (unbounded FIFO)
   *   - Real-time, no buffering? Use SynchronousQueue (direct handoff)
   *   - Urgent vs background work? Use PriorityBlockingQueue (ordered)
   */

  /**
   * Helper: Creates a ThreadFactory with customized thread names.
   * Each factory maintains its own AtomicInteger counter to assign unique IDs to threads.
   */
  private def namedFactory(prefix: String): ThreadFactory = new ThreadFactory {
    private val nextId = new AtomicInteger(1)

    override def newThread(runnable: Runnable): Thread =
      new Thread(runnable, s"$prefix-${nextId.getAndIncrement()}")
  }

  /**
   * Helper: Clean shutdown pattern — the recommended way to shut down a pool.
   */
  private def shutdownCleanly(pool: ExecutorService): Unit = {
    pool.shutdown()
    try {
      if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
        pool.shutdownNow()
        pool.awaitTermination(2, TimeUnit.SECONDS)
      }
    } catch {
      case _: InterruptedException =>
        pool.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  /**
   * Helper: Prints a visually clear section header.
   */
  private def section(number: Int, title: String): Unit = {
    println()
    println("=" * 78)
    println(s"[$number] $title")
    println("=" * 78)
  }
}

