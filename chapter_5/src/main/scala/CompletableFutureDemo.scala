import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{BiFunction, Function => JFunction, Supplier}

/*
 * Standalone CompletableFuture demo written in Scala.
 *
 * Run with:
 *   sbt "chapter5/runMain CompletableFutureDemo"
 *
 * This file demonstrates the most common CompletableFuture operations:
 *   - supplyAsync: start background work
 *   - thenApply: transform a successful value (SYNCHRONOUS - blocks completing thread)
 *   - thenApplyAsync: transform a value asynchronously on executor thread
 *   - thenAccept: consume a value without returning a result (SYNCHRONOUS)
 *   - thenAcceptAsync: consume a value asynchronously on executor thread
 *   - thenCompose: chain dependent async work
 *   - thenCombine: combine two independent async results
 *   - allOf: wait for many futures
 *   - exceptionally: recover from failure
 *   - complete: manually complete a future
 *
 * KEY INSIGHTS:
 *
 *   thenApply (SYNC):
 *     ✗ Runs on the thread that completes the previous stage
 *     ✗ Blocks that thread; thread pool starvation risk
 *     ✓ No executor overhead; faster for quick operations
 *
 *   thenApplyAsync (ASYNC):
 *     ✓ Runs on a separate executor thread
 *     ✓ Frees the completing thread immediately
 *     ✓ Better for I/O operations or long transformations
 *     ✗ Slight overhead from thread context switch
 *
 *   thenAccept (SYNC):
 *     - Like thenApply but returns CompletableFuture<Void> (no result)
 *     - Used for side effects (logging, printing, etc.)
 *     ✗ Synchronous; blocks completing thread
 *
 *   thenAcceptAsync (ASYNC):
 *     - Like thenAccept but runs on executor thread
 *     ✓ Asynchronous; frees completing thread
 *     ✓ Better for side effects that involve I/O or long operations
 */
object CompletableFutureDemo {
  private val executor: ExecutorService = Executors.newFixedThreadPool(4)

  def main(args: Array[String]): Unit = {
    try {
//      demoThenApply()
//      demoThenApplyVsThenApplyAsync()
//      demoThenAcceptVsThenAcceptAsync()
//      demoThenCompose()
//      demoThenCombine()
//      demoAllOf()
      demoExceptionally()
//      demoManualComplete()
    } finally {
      executor.shutdown()
    }
  }

  private def demoThenApply(): Unit = {
    log("\n[A] thenApply: transform one result")

    val result: CompletableFuture[String] =
      supplyAsync {
        sleep(200)
        log("loaded name")
        "sanskar"
      }.thenApply(new JFunction[String, String] {
        override def apply(name: String): String = name.toUpperCase
      })

        try {
          val res = getBlocking(result, "[A]")
          log(s"[A] result = $res")
        } catch {
          case _: RuntimeException => // already logged inside helper
        }
  }

  private def demoThenApplyVsThenApplyAsync(): Unit = {
    println("\n" + "=" * 85)
    log("COMPARISON: thenApply (SYNC) vs thenApplyAsync (ASYNC)")
    println("=" * 85)

    // =========================================================================
    // Part 1: thenApply - SYNCHRONOUS (runs on completing thread)
    // =========================================================================
    println("\n--- Part 1: thenApply (SYNCHRONOUS) ---")
    println("Transformation runs on the SAME thread that completed the previous stage.")
    println("If previous stage completes on main thread, transformation also blocks main thread.\n")

    val syncStart = System.currentTimeMillis()
    val syncResult: CompletableFuture[String] =
      supplyAsync {
        sleep(100)
        log("[Sync] Initial async work completed")
        "data"
      }.thenApply(new JFunction[String, String] {
        override def apply(data: String): String = {
          val thread = Thread.currentThread().getName
          log(s"[Sync] thenApply transformation on thread: $thread")
          sleep(100)  // Simulate transformation work
          data.toUpperCase
        }
      })

    val syncValue = try getBlocking(syncResult, "[Sync]") catch { case _: RuntimeException => "<error>" }
    val syncElapsed = System.currentTimeMillis() - syncStart
    log(s"[Sync] Result: $syncValue, Total time: ${syncElapsed}ms")
    println(s"  ✗ Completing thread is BLOCKED during transformation")
    println(s"  ✗ Risk of thread pool starvation if many sync operations stack\n")

    // =========================================================================
    // Part 2: thenApplyAsync - ASYNCHRONOUS (runs on executor thread)
    // =========================================================================
    println("--- Part 2: thenApplyAsync (ASYNCHRONOUS) ---")
    println("Transformation runs on a SEPARATE executor thread.")
    println("Completing thread is freed immediately to handle other work.\n")

    val asyncStart = System.currentTimeMillis()
    val asyncResult: CompletableFuture[String] =
      supplyAsync {
        sleep(100)
        log("[Async] Initial async work completed")
        "data"
      }.thenApplyAsync(new JFunction[String, String] {
        override def apply(data: String): String = {
          val thread = Thread.currentThread().getName
          log(s"[Async] thenApplyAsync transformation on thread: $thread")
          sleep(100)  // Simulate transformation work
          data.toUpperCase
        }
      }, executor)
    log(s"[Async] Main thread is free to do other work while transformation runs asynchronously")
    val asyncValue = try getBlocking(asyncResult, "[Async]") catch { case _: RuntimeException => "<error>" }
    val asyncElapsed = System.currentTimeMillis() - asyncStart
    log(s"[Async] Result: $asyncValue, Total time: ${asyncElapsed}ms")
    println(s"  ✓ Completing thread is freed immediately")
    println(s"  ✓ Transformation runs on worker thread from executor")
    println(s"  ✓ Better resource utilization\n")

    // =========================================================================
    // Part 3: Demonstrate the impact with chained transformations
    // =========================================================================
    println("--- Part 3: Impact of Chaining Multiple Transformations ---\n")

    println("Scenario A: Chain with thenApply (SYNC) - all on one thread")
    val chainSyncStart = System.currentTimeMillis()
    val chainSync: CompletableFuture[String] =
      supplyAsync {
        sleep(50)
        "v0"
      }.thenApply(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainSync-1] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v1"
        }
      }).thenApply(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainSync-2] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v2"
        }
      }).thenApply(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainSync-3] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v3"
        }
      })

    val chainSyncValue = try getBlocking(chainSync, "[ChainSync]") catch { case _: RuntimeException => "<error>" }
    val chainSyncElapsed = System.currentTimeMillis() - chainSyncStart
    println(s"  Result: $chainSyncValue, Time: ${chainSyncElapsed}ms")
    println(s"  ✗ All transformations run SEQUENTIALLY on single thread")
    println(s"  ✗ Total blocking time ≈ 200ms (50+50+50+50)\n")

    println("Scenario B: Chain with thenApplyAsync (ASYNC) - parallel execution possible")
    val chainAsyncStart = System.currentTimeMillis()
    val chainAsync: CompletableFuture[String] =
      supplyAsync {
        sleep(50)
        "v0"
      }.thenApplyAsync(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainAsync-1] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v1"
        }
      }, executor).thenApplyAsync(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainAsync-2] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v2"
        }
      }, executor).thenApplyAsync(new JFunction[String, String] {
        override def apply(x: String): String = {
          log(s"[ChainAsync-3] on ${Thread.currentThread().getName}")
          sleep(50)
          x + "=>v3"
        }
      }, executor)

    val chainAsyncValue = try getBlocking(chainAsync, "[ChainAsync]") catch { case _: RuntimeException => "<error>" }
    val chainAsyncElapsed = System.currentTimeMillis() - chainAsyncStart
    println(s"  Result: $chainAsyncValue, Time: ${chainAsyncElapsed}ms")
    println(s"  ✓ Transformations CAN run on different threads")
    println(s"  ✓ Better parallelism; threads freed between stages\n")

    println("=" * 85 + "\n")
  }

  private def demoThenAcceptVsThenAcceptAsync(): Unit = {
    log("\n" + "=" * 85)
    log("COMPARISON: thenAccept (SYNC) vs thenAcceptAsync (ASYNC)")
    log("=" * 85)

    println("\n--- thenAccept vs thenAcceptAsync ---")
    println("These are used for SIDE EFFECTS (logging, writing, etc.)")
    println("No result is returned; both return CompletableFuture<Void>\n")

    // =========================================================================
    // Part 1: thenAccept - SYNCHRONOUS
    // =========================================================================
    println("Scenario A: thenAccept (SYNCHRONOUS) - side effect on completing thread")
    val syncAcceptStart = System.currentTimeMillis()
    val syncAccept: CompletableFuture[Void] =
      supplyAsync {
        sleep(100)
        "important-data"
      }.thenAccept(new java.util.function.Consumer[String] {
        override def accept(data: String): Unit = {
          val thread = Thread.currentThread().getName
          log(s"[SyncAccept] Consuming '$data' on thread: $thread")
          sleep(100)  // Simulate I/O side effect (e.g., write to file)
          print("  ✗ Blocking thread during side effect\n")
        }
      })

    try {
      getBlocking(syncAccept, "[SyncAccept]")
    } catch { case _: RuntimeException => () }
    val syncAcceptElapsed = System.currentTimeMillis() - syncAcceptStart
    println(s"  Time: ${syncAcceptElapsed}ms\n")

    // =========================================================================
    // Part 2: thenAcceptAsync - ASYNCHRONOUS
    // =========================================================================
    println("Scenario B: thenAcceptAsync (ASYNCHRONOUS) - side effect on executor thread")
    val asyncAcceptStart = System.currentTimeMillis()
    val asyncAccept: CompletableFuture[Void] =
      supplyAsync {
        sleep(100)
        "important-data"
      }.thenAcceptAsync(new java.util.function.Consumer[String] {
        override def accept(data: String): Unit = {
          val thread = Thread.currentThread().getName
          log(s"[AsyncAccept] Consuming '$data' on thread: $thread")
          sleep(100)  // Simulate I/O side effect (e.g., write to file)
          print("  ✓ Non-blocking; runs on worker thread\n")
        }
      }, executor)

    try {
      getBlocking(asyncAccept, "[AsyncAccept]")
    } catch { case _: RuntimeException => () }
    val asyncAcceptElapsed = System.currentTimeMillis() - asyncAcceptStart
    println(s"  Time: ${asyncAcceptElapsed}ms\n")

    // =========================================================================
    // Part 3: Real-world scenario - multiple side effects
    // =========================================================================
    println("Scenario C: Multiple side effects (logging, metrics, notifications)")
    println("\nWith thenAccept (SYNC) - all block the main thread:")
    val multiSyncStart = System.currentTimeMillis()
    supplyAsync {
      sleep(50)
      "user-action"
    }.thenAccept(new java.util.function.Consumer[String] {
      override def accept(action: String): Unit = {
        log(s"[Sync-Log] Recording: $action")
        sleep(50)
      }
    }).thenAccept(new java.util.function.Consumer[Void] {
      override def accept(ignored: Void): Unit = {
        log("[Sync-Metrics] Updating metrics")
        sleep(50)
      }
    }).thenAccept(new java.util.function.Consumer[Void] {
      override def accept(ignored: Void): Unit = {
        log("[Sync-Notify] Sending notification")
        sleep(50)
      }
    })
    try {
      // wait for the chain to complete
      getBlocking( /* placeholder */ supplyAsync { Thread.sleep(0); null.asInstanceOf[String] }.thenAccept(new java.util.function.Consumer[String] { override def accept(s: String): Unit = () }), "[MultiSync-waiter]")
    } catch { case _: RuntimeException => () }

    val multiSyncElapsed = System.currentTimeMillis() - multiSyncStart
    println(s"  Total time with sync: ${multiSyncElapsed}ms (≈ 150ms sequential)\n")

    println("With thenAcceptAsync (ASYNC) - all can run in parallel:")
    val multiAsyncStart = System.currentTimeMillis()
    val multiAsyncChain: CompletableFuture[Void] = supplyAsync {
      sleep(50)
      "user-action"
    }.thenAcceptAsync(new java.util.function.Consumer[String] {
      override def accept(action: String): Unit = {
        log(s"[Async-Log] Recording: $action")
        sleep(50)
      }
    }, executor).thenAcceptAsync(new java.util.function.Consumer[Void] {
      override def accept(ignored: Void): Unit = {
        log("[Async-Metrics] Updating metrics")
        sleep(50)
      }
    }, executor).thenAcceptAsync(new java.util.function.Consumer[Void] {
      override def accept(ignored: Void): Unit = {
        log("[Async-Notify] Sending notification")
        sleep(50)
      }
    }, executor)
    try {
      getBlocking(multiAsyncChain, "[MultiAsync]")
    } catch { case _: RuntimeException => () }

    val multiAsyncElapsed = System.currentTimeMillis() - multiAsyncStart
    println(s"  Total time with async: ${multiAsyncElapsed}ms (can be ≈ 100ms if parallel)\n")

    println("=" * 85)
    println("SUMMARY: thenAccept vs thenAcceptAsync")
    println("=" * 85)
    println("""
      Use thenAccept when:
        ✓ Side effects are instant (no I/O)
        ✓ You trust blocking won't starve thread pool

      Use thenAcceptAsync when:
        ✓ Side effects involve I/O (logging to disk, network calls)
        ✓ Multiple side effects can run in parallel
        ✓ You want to free the main completion thread
        ✓ Dealing with high-concurrency scenarios
    """.stripMargin)
    println("=" * 85 + "\n")
  }

  private def demoThenCompose(): Unit = {
    log("\n[B] thenCompose: chain dependent async work")

    val result: CompletableFuture[String] =
      loadUserName(7).thenCompose(new JFunction[String, CompletableFuture[String]] {
        override def apply(userName: String): CompletableFuture[String] =
          loadLastOrder(userName)
      })

    try {
      val r = getBlocking(result, "[B]")
      log(s"[B] result = $r")
    } catch { case _: RuntimeException => () }
  }

  private def demoThenCombine(): Unit = {
    log("\n[C] thenCombine: combine two independent futures")

    val price: CompletableFuture[Int] = supplyAsync {
      sleep(250)
      log("loaded item price")
      800
    }

    val deliveryCharge: CompletableFuture[Int] = supplyAsync {
      sleep(150)
      log("loaded delivery charge")
      40
    }

    val total: CompletableFuture[Int] =
      price.thenCombine(deliveryCharge, new BiFunction[Int, Int, Int] {
        override def apply(itemPrice: Int, charge: Int): Int = itemPrice + charge
      })

    try {
      val t = getBlocking(total, "[C]")
      log(s"[C] total = $t")
    } catch { case _: RuntimeException => () }
  }

  private def demoAllOf(): Unit = {
    log("\n[D] allOf: wait for many futures")

    val productIds = List("book", "mouse", "monitor")
    val futures: List[CompletableFuture[String]] =
      productIds.map(loadProductPrice)

    val allDone: CompletableFuture[Void] =
      CompletableFuture.allOf(futures.toArray: _*)

    val prices: CompletableFuture[List[String]] =
      allDone.thenApply(new JFunction[Void, List[String]] {
        override def apply(ignored: Void): List[String] =
          futures.map(f => getBlocking(f, "[D-future]"))
      })

    try {
      val p = getBlocking(prices, "[D-prices]")
      log(s"[D] prices = ${p.mkString(", ")}")
    } catch { case _: RuntimeException => () }
  }

  private def demoExceptionally(): Unit = {
    log("\n[E] exceptionally: recover from failure")

    val result: CompletableFuture[String] =
      supplyAsync[String] {
        sleep(100)
        throw new IllegalStateException("service failed")
      }.exceptionally(new JFunction[Throwable, String] {
        override def apply(error: Throwable): String =
          s"fallback value because: ${error.getMessage}"
      })

    try {
      val r = getBlocking(result, "[E]")
      log(s"[E] result = $r")
    } catch { case _: RuntimeException => () }
  }

  private def demoManualComplete(): Unit = {
    log("\n[F] complete: manually finish a future")

    val future = new CompletableFuture[String]()

    executor.submit(new Runnable {
      override def run(): Unit = {
        sleep(150)
        future.complete("completed manually by worker thread")
      }
    })

    try {
      val r = getBlocking(future, "[F]")
      log(s"[F] result = $r")
    } catch { case _: RuntimeException => () }
  }


  private def getBlocking[A](future: CompletableFuture[A], label: String): A =
    try future.get()
    catch {
      case e: Exception =>
        log(s"ERROR in $label: ${e.getMessage}")
        throw new RuntimeException(s"Failed to get result for $label", e)
    }
  private def loadUserName(userId: Int): CompletableFuture[String] =
    supplyAsync {
      sleep(150)
      log(s"loaded user $userId")
      s"user-$userId"
    }

  private def loadLastOrder(userName: String): CompletableFuture[String] =
    supplyAsync {
      sleep(150)
      log(s"loaded last order for $userName")
      s"$userName last order = keyboard"
    }

  private def loadProductPrice(productId: String): CompletableFuture[String] =
    supplyAsync {
      sleep(150)
      log(s"loaded price for $productId")
      s"$productId price = ${productId.length * 100}"
    }

  private def supplyAsync[A](body: => A): CompletableFuture[A] =
    CompletableFuture.supplyAsync(new Supplier[A] {
      override def get(): A = body
    }, executor)

  private def sleep(milliseconds: Long): Unit =
    try Thread.sleep(milliseconds)
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("Interrupted while simulating work")
    }

  private def log(message: String): Unit =
    println(f"Thread Name: ${Thread.currentThread().getName}%-20s $message")
}
