/**
 * Chapter 5/7 -- Scala Future vs Java CompletableFuture vs Java Future
 *
 * Three future APIs side-by-side so you can see exactly how they map.
 *
 *   java.util.concurrent.Future      -- Java 5 (2004). Blocking-only, no composition.
 *   java.util.concurrent.CompletableFuture -- Java 8 (2014). Full async pipeline.
 *   scala.concurrent.Future          -- Scala 2.10+. Functional, immutable, composable.
 *
 * Sections:
 *   1.  Creation           Future{} (Scala)         supplyAsync()       submit(Callable)
 *   2.  Transform          map                      thenApply           N/A -- must block
 *   3.  Chain async        flatMap                  thenCompose         N/A -- must block
 *   4.  Combine two        for-comprehension        thenCombine         N/A -- must block both
 *   5.  Error recovery     recover                  exceptionally       try/catch around get()
 *   6.  Recover w/ future  recoverWith              except+thenCompose  N/A
 *   7.  Callbacks          onComplete               whenComplete        N/A -- polling only
 *   8.  Wait for all       Future.sequence          allOf               loop + join / get each
 *   9.  Manual completion  Promise                  CF (self)           N/A (read-only)
 *  10.  Already done       Future.successful        completedFuture     N/A
 *  11.  Cancel             No direct*               cf.cancel(true)     f.cancel(true)
 *  12.  isDone / poll      future.isCompleted       cf.isDone()         f.isDone()
 *  13.  Interruption       Failure on IE            cancel IGNORES      cancel DOES interrupt
 *                          (manual only)            mayInterrupt flag   running thread
 *  14.  CompletionService  onComplete+queue         thenAccept+queue    ExecutorCompletionService
 *                          (simulated)              (simulated)         (native, completion order)
 *
 * Run:
 *   sbt "chapter5/runMain ScalaVsJavaFutureDemo"
 */

import java.util.concurrent.{Callable, CompletableFuture, ExecutorCompletionService, ExecutorService, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.{Future => JFuture}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object ScalaVsJavaFutureDemo {

  // Scala Future needs an implicit ExecutionContext.
  // CompletableFuture uses ForkJoinPool.commonPool by default.
  implicit val ec: ExecutionContext = ExecutionContext.global

  // A single shared executor for all Java Future demos; shut down at the end.
  private val jExecutor: ExecutorService = Executors.newFixedThreadPool(8)

  def main(args: Array[String]): Unit = {
    try {
      section(1, "Creation")
      demoCreation()

      section(2, "Transform value  (map / thenApply / manual block+transform)")
      demoMap()

      section(3, "Chain async steps  (flatMap / thenCompose / manual sequence)")
      demoFlatMap()

      section(4, "Combine two futures  (for-comp / thenCombine / block both)")
      demoCombine()

      section(5, "Error recovery  (recover / exceptionally / try-catch get)")
      demoRecover()

      section(6, "Recover with another future  (recoverWith / except+thenCompose / N/A)")
      demoRecoverWith()

      section(7, "Side-effect callbacks  (onComplete / whenComplete / isDone polling)")
      demoCallbacks()

      section(8, "Wait for all  (Future.sequence / allOf / loop get)")
      demoWaitAll()

      section(9, "Manual completion  (Promise / CF self / N/A)")
      demoPromise()

      section(10, "Already-done futures  (successful+failed / completedFuture / N/A)")
      demoAlreadyDone()

      section(11, "Cancel  (no direct / cf.cancel / f.cancel)")
      demoCancel()

      section(12, "isDone / non-blocking poll")
      demoIsDone()

      section(13, "Interruption handling -- the KEY difference between all three")
      demoInterruption()

      section(14, "CompletionService + ExecutorPool -- process results in completion order")
      demoCompletionService()

    } finally {
      jExecutor.shutdownNow()
    }

    printSummaryTable()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 1. Creation
  // ─────────────────────────────────────────────────────────────────────────
  private def demoCreation(): Unit = {

    // SCALA Future: Future { body } submits body to ExecutionContext immediately.
    val scalaF: Future[Int] = Future {
      Thread.sleep(200)
      println(s"  [Scala  ] running on ${Thread.currentThread().getName}")
      42
    }

    // Java CompletableFuture: supplyAsync submits to ForkJoinPool.commonPool.
    val javaF: CompletableFuture[Int] = CompletableFuture.supplyAsync(() => {
      Thread.sleep(200)
      println(s"  [Java CF] running on ${Thread.currentThread().getName}")
      42
    })

    // Java Future: obtained from ExecutorService.submit(Callable).
    // There is no factory like Future{} -- you MUST have an executor.
    val jFuture: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      override def call(): Int = {
        Thread.sleep(200)
        println(s"  [Java F ] running on ${Thread.currentThread().getName}")
        42
      }
    })

    println(s"  [Scala  ] result = ${await(scalaF)}")
    println(s"  [Java CF] result = ${javaF.get(5, TimeUnit.SECONDS)}")
    println(s"  [Java F ] result = ${jFuture.get(5, TimeUnit.SECONDS)}")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 2. map / thenApply  -- transform the result value
  // ─────────────────────────────────────────────────────────────────────────
  private def demoMap(): Unit = {

    // SCALA: .map runs f on the result when it's ready, on the ExecutionContext.
    val scalaResult = await(
      Future { 10 }.map(n => {
        val r = n * n
        println(s"  [Scala  ] map: $n -> $r  (thread=${Thread.currentThread().getName})")
        r
      })
    )

    // Java CF: .thenApply runs Function<T,U> asynchronously without blocking.
    val javaResult = CompletableFuture
      .supplyAsync(() => 10)
      .thenApply(n => {
        val r = n * n
        println(s"  [Java CF] thenApply: $n -> $r  (thread=${Thread.currentThread().getName})")
        r
      })
      .get(5, TimeUnit.SECONDS)

    // Java Future: NO map/transform. You must block with get() first, then
    // apply the function yourself in the calling thread.
    val jRaw: JFuture[Int]  = jExecutor.submit(new Callable[Int] { def call() = 10 })
    val jRawVal: Int        = jRaw.get(5, TimeUnit.SECONDS)   // MUST block here
    val jFutureResult: Int  = {
      val r = jRawVal * jRawVal
      println(s"  [Java F ] manual transform (blocked): $jRawVal -> $r  (thread=${Thread.currentThread().getName})")
      r
    }

    println(s"  [Scala  ] final = $scalaResult")
    println(s"  [Java CF] final = $javaResult")
    println(s"  [Java F ] final = $jFutureResult  (no non-blocking transform -- had to block)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 3. flatMap / thenCompose  -- chain two async steps (no nested futures)
  // ─────────────────────────────────────────────────────────────────────────
  private def demoFlatMap(): Unit = {

    def fetchUserId(name: String): Future[Int] = Future {
      Thread.sleep(100); println(s"  [Scala  ] fetchUserId($name)"); name.length
    }
    def fetchScore(userId: Int): Future[String] = Future {
      Thread.sleep(100); println(s"  [Scala  ] fetchScore($userId)"); s"score=${userId * 10}"
    }

    // SCALA: flatMap chains Future[A] -> (A -> Future[B]) -> Future[B].
    val scalaResult = await(
      fetchUserId("alice").flatMap(id => fetchScore(id))
    )

    // Java CF: thenCompose is the flatMap equivalent.
    val javaResult = CompletableFuture
      .supplyAsync(() => { Thread.sleep(100); println(s"  [Java CF] fetchUserId(alice)"); 5 })
      .thenCompose(id => CompletableFuture.supplyAsync(() => {
        Thread.sleep(100)
        println(s"  [Java CF] fetchScore($id)")
        s"score=${id * 10}"
      }))
      .get(5, TimeUnit.SECONDS)

    // Java Future: NO chaining. You submit step-1, block for its result,
    // then submit step-2 manually -- entirely serial on the calling thread.
    val step1: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = { Thread.sleep(100); println(s"  [Java F ] fetchUserId(alice)"); 5 }
    })
    val userId = step1.get(5, TimeUnit.SECONDS)   // block -- kills any parallelism
    val step2: JFuture[String] = jExecutor.submit(new Callable[String] {
      def call(): String = { Thread.sleep(100); println(s"  [Java F ] fetchScore($userId)"); s"score=${userId * 10}" }
    })
    val jFutureResult = step2.get(5, TimeUnit.SECONDS)

    println(s"  [Scala  ] final = $scalaResult")
    println(s"  [Java CF] final = $javaResult")
    println(s"  [Java F ] final = $jFutureResult  (two separate submits + two blocking gets)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 4. Combine two independent futures
  // ─────────────────────────────────────────────────────────────────────────
  private def demoCombine(): Unit = {

    // SCALA: for-comprehension -- both futures start immediately (parallel).
    val scalaPrice  = Future { Thread.sleep(150); 100 }
    val scalaQty    = Future { Thread.sleep(150); 5   }
    val scalaResult = await(for { p <- scalaPrice; q <- scalaQty } yield {
      val total = p * q
      println(s"  [Scala  ] price=$p qty=$q total=$total")
      total
    })

    // Java CF: thenCombine -- both futures run in parallel; BiFunction fires
    // only when both are done.
    val javaPrice  = CompletableFuture.supplyAsync(() => { Thread.sleep(150); 100 })
    val javaQty    = CompletableFuture.supplyAsync(() => { Thread.sleep(150); 5   })
    val javaResult = javaPrice.thenCombine(javaQty, (p: Int, q: Int) => {
      val total = p * q
      println(s"  [Java CF] price=$p qty=$q total=$total")
      total
    }).get(5, TimeUnit.SECONDS)

    // Java Future: submit both first (they run in parallel), then block on
    // each individually. The actual computation overlaps; only the *waits* are serial.
    val jfPrice: JFuture[Int] = jExecutor.submit(new Callable[Int] { def call() = { Thread.sleep(150); 100 } })
    val jfQty:   JFuture[Int] = jExecutor.submit(new Callable[Int] { def call() = { Thread.sleep(150); 5   } })
    val p = jfPrice.get(5, TimeUnit.SECONDS)
    val q = jfQty.get(5, TimeUnit.SECONDS)
    val jFutureResult = p * q
    println(s"  [Java F ] price=$p qty=$q total=$jFutureResult  (parallel tasks, serial gets)")

    println(s"  [Scala  ] final = $scalaResult")
    println(s"  [Java CF] final = $javaResult")
    println(s"  [Java F ] final = $jFutureResult")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 5. recover / exceptionally  -- handle failure, return a fallback value
  // ─────────────────────────────────────────────────────────────────────────
  private def demoRecover(): Unit = {

    // SCALA: .recover takes a PartialFunction[Throwable, T]; only runs on failure.
    val scalaResult = await(
      Future[Int] { throw new RuntimeException("oops") }
        .recover { case ex: RuntimeException =>
          println(s"  [Scala  ] recover: ${ex.getMessage} -> using fallback -1")
          -1
        }
    )

    // Java CF: .exceptionally takes Function<Throwable, T>; only runs on failure.
    val javaResult = CompletableFuture
      .supplyAsync[Int](() => { throw new RuntimeException("oops") })
      .exceptionally(ex => {
        println(s"  [Java CF] exceptionally: ${ex.getCause.getMessage} -> using fallback -1")
        -1
      })
      .get(5, TimeUnit.SECONDS)

    // Java Future: NO recovery operator. Wrap get() in try/catch and handle
    // ExecutionException (which wraps the original exception as its cause).
    val jFuture: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = throw new RuntimeException("oops")
    })
    val jFutureResult: Int = try {
      jFuture.get(5, TimeUnit.SECONDS)
    } catch {
      case ex: java.util.concurrent.ExecutionException =>
        println(s"  [Java F ] ExecutionException.cause: ${ex.getCause.getMessage} -> fallback -1")
        -1
    }

    println(s"  [Scala  ] final = $scalaResult")
    println(s"  [Java CF] final = $javaResult")
    println(s"  [Java F ] final = $jFutureResult  (try/catch around get, not a pipeline operator)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 6. recoverWith / thenCompose on error  -- recover with another async step
  // ─────────────────────────────────────────────────────────────────────────
  private def demoRecoverWith(): Unit = {

    // SCALA: .recoverWith lets you return a Future[T] from the recovery.
    val scalaResult = await(
      Future[Int] { throw new RuntimeException("primary failed") }
        .recoverWith { case _: RuntimeException =>
          println(s"  [Scala  ] recoverWith: falling back to async secondary")
          Future { Thread.sleep(100); 99 }
        }
    )

    // Java CF: chain .exceptionally (wraps error as a value) then .thenCompose.
    val javaResult = CompletableFuture
      .supplyAsync[Int](() => { throw new RuntimeException("primary failed") })
      .exceptionally(ex => {
        println(s"  [Java CF] exceptionally: ${ex.getCause.getMessage}; returning sentinel")
        Int.MinValue
      })
      .thenCompose(v =>
        if (v == Int.MinValue) {
          println(s"  [Java CF] thenCompose: launching async secondary")
          CompletableFuture.supplyAsync(() => { Thread.sleep(100); 99 })
        } else CompletableFuture.completedFuture(v)
      )
      .get(5, TimeUnit.SECONDS)

    // Java Future: no async recovery operator. Catch the failure, then
    // submit a new task manually.
    val primary: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = throw new RuntimeException("primary failed")
    })
    val jFutureResult: Int = try {
      primary.get(5, TimeUnit.SECONDS)
    } catch {
      case _: java.util.concurrent.ExecutionException =>
        println(s"  [Java F ] primary failed; submitting fallback task manually")
        val fallback: JFuture[Int] = jExecutor.submit(new Callable[Int] {
          def call(): Int = { Thread.sleep(100); 99 }
        })
        fallback.get(5, TimeUnit.SECONDS)
    }

    println(s"  [Scala  ] final = $scalaResult")
    println(s"  [Java CF] final = $javaResult")
    println(s"  [Java F ] final = $jFutureResult  (catch + re-submit; no pipeline)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 7. Callbacks / notifications
  // ─────────────────────────────────────────────────────────────────────────
  private def demoCallbacks(): Unit = {

    // SCALA: .onComplete fires on Success or Failure; runs on ExecutionContext.
    val scalaF = Future { Thread.sleep(200); "hello" }
    scalaF.onComplete {
      case Success(v)  => println(s"  [Scala  ] onComplete Success: $v")
      case Failure(ex) => println(s"  [Scala  ] onComplete Failure: ${ex.getMessage}")
    }

    // Java CF: .whenComplete fires with (result, exception); pass-through return.
    val javaF = CompletableFuture
      .supplyAsync(() => { Thread.sleep(200); "hello" })
      .whenComplete((v, ex) =>
        if (ex == null) println(s"  [Java CF] whenComplete success: $v")
        else            println(s"  [Java CF] whenComplete failure: ${ex.getMessage}")
      )

    // Java Future: NO callback/listener API at all.
    // Only option: poll isDone() in a loop, or block with get().
    // We show the polling pattern here.
    val jFuture: JFuture[String] = jExecutor.submit(new Callable[String] {
      def call(): String = { Thread.sleep(200); "hello" }
    })
    new Thread(() => {
      while (!jFuture.isDone) {
        println(s"  [Java F ] polling isDone()=false -- sleeping 50 ms")
        Thread.sleep(50)
      }
      val result = jFuture.get()
      println(s"  [Java F ] isDone()=true, polled result: $result")
    }, "jf-poller").start()

    await(scalaF)
    javaF.get(5, TimeUnit.SECONDS)
    Thread.sleep(300)   // let poller finish
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 8. Wait for all
  // ─────────────────────────────────────────────────────────────────────────
  private def demoWaitAll(): Unit = {
    val ids = List(1, 2, 3)

    // SCALA: Future.sequence -- fails fast if any future fails.
    val scalaFutures = ids.map(i => Future { Thread.sleep(100); i * 10 })
    val scalaAll     = await(Future.sequence(scalaFutures))
    println(s"  [Scala  ] Future.sequence result = $scalaAll")

    // Java CF: allOf -- no return value, must retrieve manually after.
    val javaFutures = ids.map(i => CompletableFuture.supplyAsync(() => { Thread.sleep(100); i * 10 }))
    CompletableFuture.allOf(javaFutures: _*).get(5, TimeUnit.SECONDS)
    val javaAll = javaFutures.map(_.get())
    println(s"  [Java CF] allOf result           = $javaAll")

    // Java Future: submit all, then loop get() on each -- tasks run in parallel,
    // gets are sequential but that's fine since the slower ones are already done.
    val jFutures = ids.map(i => jExecutor.submit(new Callable[Int] {
      def call(): Int = { Thread.sleep(100); i * 10 }
    }))
    val jFutureAll = jFutures.map(_.get(5, TimeUnit.SECONDS))
    println(s"  [Java F ] loop get() result      = $jFutureAll  (parallel tasks, serial gets)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 9. Manual completion  (Promise / CF self / N/A for Java Future)
  // ─────────────────────────────────────────────────────────────────────────
  private def demoPromise(): Unit = {

    // SCALA: Promise is the write side; promise.future is the read side.
    val promise = Promise[String]()
    val scalaF  = promise.future
    new Thread(() => {
      Thread.sleep(200)
      println(s"  [Scala  ] completing promise with 'done'")
      promise.success("done")
    }, "promise-completer").start()
    println(s"  [Scala  ] waiting for promise...")
    println(s"  [Scala  ] got = ${await(scalaF)}")

    // Java CF: CompletableFuture IS both promise and future -- call complete()
    // from any thread to unblock anyone calling get().
    val javaPromise = new CompletableFuture[String]()
    new Thread(() => {
      Thread.sleep(200)
      println(s"  [Java CF] completing CompletableFuture with 'done'")
      javaPromise.complete("done")
    }, "cf-completer").start()
    println(s"  [Java CF] waiting for CompletableFuture...")
    println(s"  [Java CF] got = ${javaPromise.get(5, TimeUnit.SECONDS)}")

    // Java Future: read-only. The result is fixed at submit time.
    // There is NO way to complete a JFuture from outside -- you cannot
    // write to it after it has been handed back to you.
    println(s"  [Java F ] N/A -- java.util.concurrent.Future is read-only; use CF or Promise instead")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 10. Already-done futures  (no async work needed)
  // ─────────────────────────────────────────────────────────────────────────
  private def demoAlreadyDone(): Unit = {

    // SCALA
    val scalaOk  = Future.successful(42)
    val scalaErr = Future.failed[Int](new RuntimeException("pre-failed"))
    println(s"  [Scala  ] successful  = ${await(scalaOk)}")
    println(s"  [Scala  ] failed      = ${Try(await(scalaErr)).failed.get.getMessage}")

    // Java CF
    val javaOk  = CompletableFuture.completedFuture(42)
    val javaErr = CompletableFuture.failedFuture[Int](new RuntimeException("pre-failed"))
    println(s"  [Java CF] completedFuture = ${javaOk.get()}")
    println(s"  [Java CF] failedFuture    = ${Try(javaErr.get()).failed.get.getCause.getMessage}")

    // Java Future: no factory for pre-completed futures.
    // Closest workaround: submit a no-op that immediately returns a value.
    val jfOk: JFuture[Int] = jExecutor.submit(new Callable[Int] { def call() = 42 })
    println(s"  [Java F ] no factory; submit no-op = ${jfOk.get(5, TimeUnit.SECONDS)}")
    println(s"  [Java F ] failedFuture = N/A (no equivalent; must catch ExecutionException on get)")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 11. Cancel
  // ─────────────────────────────────────────────────────────────────────────
  private def demoCancel(): Unit = {

    // SCALA: scala.concurrent.Future has NO cancel(). The only way is to
    // use a shared cancellation flag (like PrimeGeneratorCancellationDemo)
    // or wrap a Java Future/CF and cancel that. Shown for completeness.
    println(s"  [Scala  ] No cancel() on Scala Future -- use AtomicBoolean flag or wrap a CF")

    // Java CF: cancel(mayInterruptIfRunning) -- cancels if not yet started,
    // OR interrupts the running thread if true.  Subsequent get() throws CancellationException.
    val javaF: CompletableFuture[Int] = CompletableFuture.supplyAsync(() => {
      Thread.sleep(500); 42
    })
    Thread.sleep(100)
    val cancelled = javaF.cancel(true)
    val javaResult = Try(javaF.get(2, TimeUnit.SECONDS)) match {
      case Failure(_: java.util.concurrent.CancellationException) => "CancellationException (correct)"
      case Success(v) => s"got $v (not cancelled?)"
      case Failure(ex) => s"other error: ${ex.getMessage}"
    }
    println(s"  [Java CF] cancel(true) returned=$cancelled; get() threw: $javaResult")

    // Java Future: cancel(mayInterruptIfRunning) on the interface -- same semantics.
    val jFuture: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = { Thread.sleep(500); 42 }
    })
    Thread.sleep(100)
    val jfCancelled = jFuture.cancel(true)
    val jfResult = Try(jFuture.get(2, TimeUnit.SECONDS)) match {
      case Failure(_: java.util.concurrent.CancellationException) => "CancellationException (correct)"
      case Success(v) => s"got $v"
      case Failure(ex) => s"other: ${ex.getMessage}"
    }
    println(s"  [Java F ] cancel(true) returned=$jfCancelled; get() threw: $jfResult")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 12. isDone / non-blocking poll
  // ─────────────────────────────────────────────────────────────────────────
  private def demoIsDone(): Unit = {

    // SCALA: future.isCompleted (non-blocking); future.value returns Option[Try[T]]
    val scalaF = Future { Thread.sleep(300); 7 }
    println(s"  [Scala  ] immediately: isCompleted=${scalaF.isCompleted}  value=${scalaF.value}")
    Thread.sleep(400)
    println(s"  [Scala  ] after 400ms: isCompleted=${scalaF.isCompleted}  value=${scalaF.value}")

    // Java CF: isDone() covers success, failure, AND cancellation.
    val javaF = CompletableFuture.supplyAsync(() => { Thread.sleep(300); 7 })
    println(s"  [Java CF] immediately: isDone=${javaF.isDone}")
    Thread.sleep(400)
    println(s"  [Java CF] after 400ms: isDone=${javaF.isDone}  get=${javaF.get()}")

    // Java Future: isDone() same semantics.
    val jFuture: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = { Thread.sleep(300); 7 }
    })
    println(s"  [Java F ] immediately: isDone=${jFuture.isDone}")
    Thread.sleep(400)
    println(s"  [Java F ] after 400ms: isDone=${jFuture.isDone}  get=${jFuture.get(1, TimeUnit.SECONDS)}")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 13. Interruption handling
  //
  //  THE CRITICAL SURPRISE:
  //   Java Future  cancel(true) => thread.interrupt() is actually called.
  //                                Sleep/blocking-op throws InterruptedException.
  //                                Task exits early.
  //
  //   CompletableFuture cancel(true) => mayInterruptIfRunning is IGNORED.
  //                                get() caller sees CancellationException fast,
  //                                but the task thread keeps running the full body.
  //
  //   Scala Future   => NO cancel() at all. Only way to stop is:
  //                       (a) hold the Thread ref and call interrupt() manually, or
  //                       (b) use a cooperative AtomicBoolean flag inside the task.
  //                     InterruptedException thrown inside the body becomes Failure.
  // ─────────────────────────────────────────────────────────────────────────
  private def demoInterruption(): Unit = {
    import java.util.concurrent.CountDownLatch
    import java.util.concurrent.atomic.AtomicLong

    // ── A. Java Future cancel(true): THREAD IS INTERRUPTED ───────────────
    println()
    println("  ── A. Java Future  cancel(true) ── thread IS interrupted ──")

    val jfTaskFinishMs = new AtomicLong(-1)
    val jfStart        = System.currentTimeMillis()

    val jfTask: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = {
        println(s"  [Java F ] task started; sleeping 1500ms")
        try {
          Thread.sleep(1500)
          println(s"  [Java F ] task completed NORMALLY -- was NOT interrupted")
          99
        } catch {
          case _: InterruptedException =>
            val ms = System.currentTimeMillis() - jfStart
            println(s"  [Java F ] task caught InterruptedException at ${ms}ms -- exits early; restoring flag")
            Thread.currentThread().interrupt()  // restore so caller can observe
            -1
        } finally {
          jfTaskFinishMs.set(System.currentTimeMillis() - jfStart)
        }
      }
    })

    Thread.sleep(300)
    println(s"  [Java F ] cancel(true) called at ~300ms")
    jfTask.cancel(true)
    Thread.sleep(400)   // wait for task to react
    println(s"  [Java F ] task finished at: ${jfTaskFinishMs.get}ms  (expected ~300ms, NOT 1500ms)")
    val jfGetEx = Try(jfTask.get(1, TimeUnit.SECONDS)).failed.map(_.getClass.getSimpleName).getOrElse("no error")
    println(s"  [Java F ] get() threw: $jfGetEx")

    // ── B. CompletableFuture cancel(true): mayInterruptIfRunning IGNORED ──
    println()
    println("  ── B. CompletableFuture  cancel(true) ── thread is NOT interrupted ──")
    println("       get() caller unblocks immediately with CancellationException,")
    println("       but the task thread keeps running the full 1500ms body!")

    val cfTaskFinishMs = new AtomicLong(-1)
    val cfStart        = System.currentTimeMillis()

    val cfTask: CompletableFuture[Int] = CompletableFuture.supplyAsync(() => {
      println(s"  [Java CF] task started; sleeping 1500ms")
      try {
        Thread.sleep(1500)
        println(s"  [Java CF] task ran to completion at ${System.currentTimeMillis() - cfStart}ms -- NOT interrupted!")
      } catch {
        case _: InterruptedException =>
          println(s"  [Java CF] task WAS interrupted (this would be surprising)")
      }
      cfTaskFinishMs.set(System.currentTimeMillis() - cfStart)
      42
    })

    Thread.sleep(300)
    println(s"  [Java CF] cancel(true) called at ~300ms")
    cfTask.cancel(true)

    val getStart     = System.currentTimeMillis()
    val cfGetEx      = Try(cfTask.get(2, TimeUnit.SECONDS)).failed.map(_.getClass.getSimpleName).getOrElse("no error")
    val getElapsedMs = System.currentTimeMillis() - getStart
    println(s"  [Java CF] get() threw $cfGetEx in ${getElapsedMs}ms  (fast -- future was already cancelled)")

    Thread.sleep(1500)   // wait for the task thread to actually finish
    println(s"  [Java CF] task body actually finished at: ${cfTaskFinishMs.get}ms")
    println(s"  [Java CF] *** cancel() made get() throw fast, but task thread kept consuming CPU/resources ***")

    // ── C. Scala Future: no cancel(); interrupt via thread ref; IE -> Failure ─
    println()
    println("  ── C. Scala Future ── no cancel(); InterruptedException becomes Failure ──")

    val taskStarted    = new CountDownLatch(1)
    @volatile var taskThread: Thread = null

    val scalaF: Future[Int] = Future {
      taskThread = Thread.currentThread()
      taskStarted.countDown()
      println(s"  [Scala  ] task started on ${Thread.currentThread().getName}; sleeping 1500ms")
      try {
        Thread.sleep(1500)
        println(s"  [Scala  ] task completed normally")
        77
      } catch {
        case ie: InterruptedException =>
          val ms = System.currentTimeMillis()
          println(s"  [Scala  ] InterruptedException caught -- re-throwing so Future captures as Failure")
          Thread.currentThread().interrupt()
          throw ie   // Future wraps this as Failure[InterruptedException]
      }
    }

    taskStarted.await()   // ensure taskThread is set
    Thread.sleep(100)
    println(s"  [Scala  ] no cancel() API -- manually calling taskThread.interrupt()")
    taskThread.interrupt()

    Thread.sleep(300)
    val scalaResult = Try(Await.result(scalaF, 3.seconds))
    scalaResult match {
      case Success(v)  => println(s"  [Scala  ] result = Success($v)  (ran to completion)")
      case Failure(ex) => println(s"  [Scala  ] result = Failure(${ex.getClass.getSimpleName}) -- IE surfaced as Failure")
    }
    println(s"  [Scala  ] Preferred alternative: AtomicBoolean flag inside the task (cooperative cancellation)")

    // ── D. Proper vs bad InterruptedException handling inside a Callable ──
    println()
    println("  ── D. Inside a task: good (re-throw IE) vs bad (swallow IE) ──")

    // Good: re-throw; interrupt flag is preserved for the thread pool worker loop
    val goodTask: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = {
        try { Thread.sleep(1000); 1 }
        catch {
          case ie: InterruptedException =>
            println(s"  [Java F ] GOOD task: caught IE, restoring flag and re-throwing")
            Thread.currentThread().interrupt()
            throw ie
        }
      }
    })
    Thread.sleep(100); goodTask.cancel(true); Thread.sleep(200)
    val goodEx = Try(goodTask.get(1, TimeUnit.SECONDS)).failed
      .map(ex => s"${ex.getClass.getSimpleName}").getOrElse("no error")
    println(s"  [Java F ] GOOD task get() threw: $goodEx")

    // Bad: swallow IE; the thread keeps looping even after cancel(true)
    val badTaskRan = new AtomicLong(0)
    val badTask: JFuture[Int] = jExecutor.submit(new Callable[Int] {
      def call(): Int = {
        var i = 0
        while (i < 15) {
          try { Thread.sleep(100); i += 1; badTaskRan.set(i) }
          catch {
            case _: InterruptedException =>
              println(s"  [Java F ] BAD task: swallowed IE at i=$i -- interrupt flag cleared; loop continues!")
              // BUG: flag cleared, worker loop and pool shutdown can't observe it
          }
        }
        println(s"  [Java F ] BAD task: finished all 15 iterations despite cancel!")
        i
      }
    })
    Thread.sleep(150); badTask.cancel(true)
    Thread.sleep(1800)  // observe that bad task keeps running
    println(s"  [Java F ] BAD task ran ${ badTaskRan.get } iterations (expected ~1, ran all 15)")
    println(s"  [Java F ] get() on cancelled future = ${Try(badTask.get(1, TimeUnit.SECONDS)).failed.map(_.getClass.getSimpleName).getOrElse("no error")}")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 14. CompletionService + ExecutorPool
  //
  //  PROBLEM:  Submit N tasks to a pool and block on each in submission order.
  //            Even if task-4 finishes in 50ms you wait for task-1 (400ms)
  //            before processing task-4's result.
  //
  //  SOLUTION: ExecutorCompletionService wraps a pool and maintains an
  //            internal completion queue. take() returns the NEXT completed
  //            future, whatever order they finished in.
  //
  //   A. Without CompletionService  -- results consumed in SUBMISSION order
  //   B. Java  ExecutorCompletionService -- results consumed in COMPLETION order
  //   C. CompletableFuture equivalent   -- thenAccept callbacks + LinkedBlockingQueue
  //   D. Scala Future equivalent        -- onComplete callbacks + LinkedBlockingQueue
  //                                        (or Future.firstCompletedOf for first-done)
  // ─────────────────────────────────────────────────────────────────────────
  private def demoCompletionService(): Unit = {

    // Five tasks with deliberately uneven durations so completion order differs
    // from submission order.
    // Submission:  task-1  task-2  task-3  task-4  task-5
    // Sleep (ms):    400     100     250      50     300
    // Finish order: task-4, task-2, task-3, task-5, task-1
    case class Task(id: Int, sleepMs: Int)
    val tasks = List(Task(1,400), Task(2,100), Task(3,250), Task(4,50), Task(5,300))

    // ── A. WITHOUT CompletionService ─────────────────────────────────────
    println()
    println("  ── A. WITHOUT CompletionService -- blocked in submission order ──")
    val poolA: ExecutorService = Executors.newFixedThreadPool(5)
    try {
      val startA = System.currentTimeMillis()
      // Submit all; collect futures in submission order
      val futures: List[JFuture[String]] = tasks.map { t =>
        poolA.submit(new Callable[String] {
          def call(): String = { Thread.sleep(t.sleepMs); s"task-${t.id}" }
        })
      }
      // Drain in submission order -- we wait for task-1 first even though
      // task-4 finished 350ms ago
      futures.foreach { f =>
        val result = f.get()
        println(s"  [No-CS  ] got $result at +${System.currentTimeMillis() - startA}ms")
      }
      println(s"  [No-CS  ] total: ${System.currentTimeMillis() - startA}ms  (gated by task-1 at ~400ms)")
    } finally { poolA.shutdown() }

    // ── B. Java ExecutorCompletionService ────────────────────────────────
    println()
    println("  ── B. Java ExecutorCompletionService -- results in COMPLETION order ──")
    val poolB: ExecutorService = Executors.newFixedThreadPool(5)
    val cs = new ExecutorCompletionService[String](poolB)
    try {
      val startB = System.currentTimeMillis()
      // Submit all tasks to the completion service
      tasks.foreach { t =>
        cs.submit(new Callable[String] {
          def call(): String = { Thread.sleep(t.sleepMs); s"task-${t.id}" }
        })
      }
      // take() blocks only until the NEXT task completes -- no wasted waiting
      (1 to tasks.size).foreach { _ =>
        val done   = cs.take()          // blocks until any task finishes
        val result = done.get()         // never blocks; task is already done
        println(s"  [CS     ] got $result at +${System.currentTimeMillis() - startB}ms")
      }
      println(s"  [CS     ] total: ${System.currentTimeMillis() - startB}ms  (same wall-clock, but each result processed immediately)")
    } finally { poolB.shutdown() }

    // ── C. CompletableFuture equivalent ──────────────────────────────────
    println()
    println("  ── C. CompletableFuture -- thenAccept callbacks feed a LinkedBlockingQueue ──")
    val startC    = System.currentTimeMillis()
    val cfQueue   = new LinkedBlockingQueue[String]()
    // Each CF puts its result into the queue as soon as it completes.
    // No blocking on the submitting thread; the queue delivers completion order.
    tasks.foreach { t =>
      CompletableFuture
        .supplyAsync(() => { Thread.sleep(t.sleepMs); s"task-${t.id}" })
        .thenAccept(r => cfQueue.put(r))   // fires on completion thread
    }
    (1 to tasks.size).foreach { _ =>
      val result = cfQueue.take()          // blocks until next result arrives
      println(s"  [CF-CS  ] got $result at +${System.currentTimeMillis() - startC}ms")
    }
    println(s"  [CF-CS  ] total: ${System.currentTimeMillis() - startC}ms")

    // ── D. Scala Future equivalent ────────────────────────────────────────
    println()
    println("  ── D. Scala Future -- onComplete callbacks feed a LinkedBlockingQueue ──")
    val startD  = System.currentTimeMillis()
    val sfQueue = new LinkedBlockingQueue[String]()
    // onComplete fires on the ExecutionContext thread when the future finishes.
    // We put the result into the queue, and the main thread drains it in order.
    tasks.foreach { t =>
      Future { Thread.sleep(t.sleepMs); s"task-${t.id}" }
        .onComplete {
          case Success(r)  => sfQueue.put(r)
          case Failure(ex) => sfQueue.put(s"ERROR: ${ex.getMessage}")
        }
    }
    (1 to tasks.size).foreach { _ =>
      val result = sfQueue.take()
      println(s"  [SF-CS  ] got $result at +${System.currentTimeMillis() - startD}ms")
    }
    println(s"  [SF-CS  ] total: ${System.currentTimeMillis() - startD}ms")

    // ── E. Scala firstCompletedOf -- get ONLY the first done ─────────────
    println()
    println("  ── E. Scala Future.firstCompletedOf -- returns the single fastest result ──")
    val startE    = System.currentTimeMillis()
    val raceFutures = tasks.map(t => Future { Thread.sleep(t.sleepMs); s"task-${t.id}" })
    val winner      = await(Future.firstCompletedOf(raceFutures))
    println(s"  [SF-1st ] first completed: $winner at +${System.currentTimeMillis() - startE}ms  (others still running)")

    // CompletableFuture.anyOf equivalent
    val cfRaces = tasks.map(t =>
      CompletableFuture.supplyAsync[AnyRef](() => { Thread.sleep(t.sleepMs); s"task-${t.id}" })
    )
    val startE2   = System.currentTimeMillis()
    val cfWinner  = CompletableFuture.anyOf(cfRaces: _*).get(5, TimeUnit.SECONDS)
    println(s"  [CF-any ] anyOf first:     $cfWinner at +${System.currentTimeMillis() - startE2}ms")

    println()
    println("  Summary:")
    println("   A. No CompletionService  => wait for task-1 before seeing task-4 result (+400ms)")
    println("   B. ExecutorCompletionService => take() gives next-done, task-4 processed at +50ms")
    println("   C. CompletableFuture + thenAccept + queue => same completion-order behaviour")
    println("   D. Scala Future + onComplete + queue      => same completion-order behaviour")
    println("   E. firstCompletedOf / anyOf               => get the single fastest result only")
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Summary table -- all three APIs
  // ─────────────────────────────────────────────────────────────────────────
  private def printSummaryTable(): Unit = {
    println()
    println("=" * 100)
    println("  COMPARISON SUMMARY  (Scala Future  |  Java CompletableFuture  |  Java Future)")
    println("=" * 100)
    // columns: concept | Scala Future | Java CF | Java Future
    val rows = List(
      ("Concept",            "Scala Future",              "Java CompletableFuture",        "Java Future (juc)"),
      ("-" * 22,             "-" * 26,                    "-" * 30,                        "-" * 18),
      ("Introduced",         "Scala 2.10 (2012)",         "Java 8 (2014)",                 "Java 5 (2004)"),
      ("Create",             "Future { body }",           "supplyAsync(() -> body)",       "executor.submit(callable)"),
      ("Already done",       "Future.successful(v)",      "completedFuture(v)",            "N/A (submit no-op)"),
      ("Already failed",     "Future.failed(ex)",         "failedFuture(ex)",              "N/A"),
      ("Transform (map)",    ".map(f)",                   ".thenApply(f)",                 "NO -- block + apply manually"),
      ("Chain async",        ".flatMap(f)",               ".thenCompose(f)",               "NO -- block + re-submit"),
      ("Combine two",        "for { a<-fa; b<-fb }",      ".thenCombine(other, f)",        "submit both, block each"),
      ("Recover value",      ".recover { case e => v }",  ".exceptionally(e -> v)",        "try/catch around get()"),
      ("Recover w/ future",  ".recoverWith { case e=>f}", ".exceptionally+thenCompose",    "catch + re-submit"),
      ("Callback",           ".onComplete { case ...}",   ".whenComplete((v,e) -> ...)",   "NO -- poll isDone() only"),
      ("Wait for all",       "Future.sequence(list)",     "allOf(futures*) [no vals]",     "loop + get() each"),
      ("Manual completion",  "Promise[T].success(v)",     ".complete(v)  [CF=self]",       "NO -- read-only"),
      ("Cancel",             "NO (use AtomicBoolean)",    ".cancel(interrupt)",            ".cancel(interrupt)"),
      ("Poll non-blocking",  ".isCompleted / .value",     ".isDone()",                     ".isDone()"),
      ("Block for result",   "Await.result(f, dur)",      "f.get(timeout, unit)",          "f.get(timeout, unit)"),
      ("Execution context",  "implicit ExecutionContext",  "ForkJoinPool.commonPool",       "Provided ExecutorService"),
      ("Composable",         "YES",                       "YES",                           "NO"),
      ("Mutable / writable", "NO (use Promise)",          "YES (CF is its own promise)",   "NO (read-only handle)"),
      ("Fail-fast sequence", "YES (Future.sequence)",     "YES (allOf propagates)",        "NO (must check each)"),
      ("-" * 22,             "-" * 26,                    "-" * 30,                        "-" * 18),
      ("cancel() interrupts","NO cancel() at all",        "NO -- flag IGNORED (surprise!)", "YES -- thread.interrupt()"),
      ("IE in body",         "Becomes Failure[IE]",       "CF completes exceptionally",    "Wraps in ExecutionException"),
      ("Stop running task",  "AtomicBoolean flag or",     "Impossible via CF API --",      "cancel(true) interrupts"),
      ("",                   "hold Thread ref + interrupt","task thread keeps running",     "the thread directly"),
      ("-" * 22,             "-" * 26,                    "-" * 30,                        "-" * 18),
      ("Completion order",   "onComplete + queue",        "thenAccept + queue",            "ExecutorCompletionService"),
      ("Native API",         "NO (simulate with queue)",  "NO (simulate with queue)",      "YES -- cs.take() / cs.poll()"),
      ("First-done only",    "Future.firstCompletedOf",   "CompletableFuture.anyOf()",     "cs.take() (first result)")
    )
    rows.foreach { case (a, b, c, d) =>
      println(f"  ${a}%-22s  ${b}%-28s  ${c}%-32s  $d")
    }
    println("=" * 100)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────
  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  private def section(n: Int, title: String): Unit = {
    println()
    println(s"─" * 70)
    println(s"  [$n] $title")
    println(s"─" * 70)
  }
}
