import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{BiFunction, Function => JFunction, Supplier}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration._

/*
 * CompletableFutureWithScalaFuture
 *
 * Purpose:
 *
 *   This file explains the same asynchronous workflow in two ways:
 *
 *   1. Java CompletableFuture used from Scala.
 *   2. Native scala.concurrent.Future and Promise.
 *
 * Run the CompletableFuture version:
 *
 *   sbt "chapter5/runMain CompletableFutureWithScalaFuture"
 *
 * Run the Scala Future version:
 *
 *   sbt "chapter5/runMain ScalaFutureEquivalentForCompletableFuture"
 *
 * Why this comparison is useful:
 *
 *   CompletableFuture is a Java API, so Scala code must often pass Java
 *   functional interfaces such as Supplier, Function, and BiFunction.
 *   Scala Future is the idiomatic Scala API, so the same ideas are usually
 *   expressed with map, flatMap, zip, Future.sequence, recover, and Promise.
 *
 * Feature mapping:
 *
 *   CompletableFuture syntax                  Scala Future equivalent
 *
 *   supplyAsync { ... }                       Future { ... }
 *   thenApply(a => b)                         future.map(a => b)
 *   thenCompose(a => CompletableFuture[B])    future.flatMap(a => Future[B])
 *   thenCombine(other, combine)               future.zip(other).map(...)
 *   CompletableFuture.allOf(...)              Future.sequence(...)
 *   exceptionally(error => fallback)          future.recover { case error => ... }
 *   new CompletableFuture[A]().complete(v)    Promise[A]().success(v)
 *   future.join()                             Await.result(future, timeout)
 *
 * Demos implemented below:
 *
 *   [A] demoPipeline()
 *       Starts async work, transforms the result, then starts dependent async
 *       work. This demonstrates supplyAsync, thenApply, and thenCompose.
 *
 *   [B] demoCombine()
 *       Starts two independent async computations in parallel and combines
 *       their values after both complete. This demonstrates thenCombine.
 *
 *   [C] demoAllOf()
 *       Starts many futures and waits until all of them finish. This
 *       demonstrates CompletableFuture.allOf and Future.sequence.
 *
 *   [D] demoExceptionHandling()
 *       Converts a failed async computation into a fallback value. This
 *       demonstrates exceptionally and recover.
 *
 *   [E] demoManualCompletion()
 *       Creates an empty future and completes it later from another thread.
 *       This demonstrates CompletableFuture.complete and Promise.success.
 *
 * Notes:
 *
 *   join and Await.result block the current thread. They are used here only to
 *   keep the console demo simple. In real applications, prefer returning the
 *   future and composing it with map, flatMap, thenApply, or thenCompose.
 *
 *   This file passes an explicit ExecutorService. If no executor is passed to
 *   CompletableFuture async methods, Java uses ForkJoinPool.commonPool().
 */
object CompletableFutureWithScalaFuture {
  // A fixed thread pool makes it easy to see work running on background threads.
  private val executor: ExecutorService = Executors.newFixedThreadPool(4)

  def main(args: Array[String]): Unit = {
    try {
      demoPipeline()
      demoCombine()
      demoAllOf()
      demoExceptionHandling()
      demoManualCompletion()
    } finally {
      executor.shutdown()
    }
  }

  private def demoPipeline(): Unit = {
    log("\n[A] supplyAsync + thenApply + thenCompose")

    val profile: CompletableFuture[String] =
      // Step 1: start an async computation that returns a user name.
      supplyAsync {
        sleep(200)
        log("Loaded user name")
        "sanskar"
      }.thenApply(new JFunction[String, String] {
        // Step 2: transform the completed result without starting another task.
        override def apply(userName: String): String = userName.toUpperCase
      }).thenCompose(new JFunction[String, CompletableFuture[String]] {
        // Step 3: start another async task that depends on the previous result.
        override def apply(userName: String): CompletableFuture[String] =
          supplyAsync {
            sleep(200)
            log(s"Loaded score for $userName")
            s"$userName score = ${userName.length * 10}"
          }
      })

    // join is used here only so the console demo waits for the async result.
    log(s"[A] result: ${profile.join()}")
  }

  private def demoCombine(): Unit = {
    log("\n[B] thenCombine joins two independent futures")

    // price and deliveryCharge run at the same time because neither depends on the other.
    val price: CompletableFuture[Int] = supplyAsync {
      sleep(200)
      log("Fetched item price")
      800
    }

    val deliveryCharge: CompletableFuture[Int] = supplyAsync {
      sleep(150)
      log("Fetched delivery charge")
      40
    }

    val total: CompletableFuture[Int] =
      price.thenCombine(deliveryCharge, new BiFunction[Int, Int, Int] {
        // This function runs after both futures complete successfully.
        override def apply(itemPrice: Int, charge: Int): Int = itemPrice + charge
      })

    log(s"[B] total amount = ${total.join()}")
  }

  private def demoAllOf(): Unit = {
    log("\n[C] allOf waits until every future is complete")

    val productIds = List("book", "mouse", "monitor")
    val futures: List[CompletableFuture[String]] =
      productIds.map { productId =>
        // Every item becomes its own CompletableFuture[String].
        supplyAsync {
          sleep(200)
          log(s"Fetched price for $productId")
          s"$productId price = ${productId.length * 100}"
        }
      }

    val allDone: CompletableFuture[Void] =
      // allOf returns CompletableFuture[Void], so the results are collected separately.
      CompletableFuture.allOf(futures.toArray: _*)

    val prices: CompletableFuture[List[String]] =
      allDone.thenApply(new JFunction[Void, List[String]] {
        override def apply(ignored: Void): List[String] =
          futures.map(_.join())
      })

    log(s"[C] prices: ${prices.join().mkString(", ")}")
  }

  private def demoExceptionHandling(): Unit = {
    log("\n[D] exceptionally converts a failure into a fallback value")

    val safeResult: CompletableFuture[String] =
      // The type parameter is written explicitly because this branch always throws.
      supplyAsync[String] {
        sleep(100)
        throw new IllegalStateException("remote service failed")
      }.exceptionally(new JFunction[Throwable, String] {
        override def apply(error: Throwable): String =
          s"fallback value because: ${error.getMessage}"
      })

    log(s"[D] result: ${safeResult.join()}")
  }

  private def demoManualCompletion(): Unit = {
    log("\n[E] complete manually finishes a future")

    // Manual futures are useful when some callback-style API will supply the value later.
    val future = new CompletableFuture[String]()

    executor.submit(new Runnable {
      override def run(): Unit = {
        sleep(150)
        future.complete("completed by worker thread")
      }
    })

    log(s"[E] result: ${future.join()}")
  }

  private def supplyAsync[A](body: => A): CompletableFuture[A] =
    // Scala block syntax is converted into Java's Supplier interface here.
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
    println(f"${Thread.currentThread().getName}%-20s $message")
}

/*
 * ScalaFutureEquivalentForCompletableFuture
 *
 * This object intentionally mirrors CompletableFutureWithScalaFuture. Compare
 * methods with the same names in both objects to see how the API style changes:
 *
 *   - CompletableFuture uses Java interfaces: Supplier, JFunction, BiFunction.
 *   - Scala Future uses Scala functions: value => ..., case (...), for/map style.
 *   - CompletableFuture manual completion uses complete(value).
 *   - Scala Future manual completion uses Promise, then exposes promise.future.
 */
object ScalaFutureEquivalentForCompletableFuture {
  private implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  def main(args: Array[String]): Unit = {
    try {
      demoPipeline()
      demoCombine()
      demoAllOf()
      demoExceptionHandling()
      demoManualCompletion()
    } finally {
      ec.shutdown()
    }
  }

  private def demoPipeline(): Unit = {
    log("\n[A] Future + map + flatMap")

    val profile: Future[String] =
      Future {
        sleep(200)
        log("Loaded user name")
        "sanskar"
      }.map { userName =>
        userName.toUpperCase
      }.flatMap { userName =>
        Future {
          sleep(200)
          log(s"Loaded score for $userName")
          s"$userName score = ${userName.length * 10}"
        }
      }

    log(s"[A] result: ${Await.result(profile, 2.seconds)}")
  }

  private def demoCombine(): Unit = {
    log("\n[B] zip joins two independent futures")

    val price: Future[Int] = Future {
      sleep(200)
      log("Fetched item price")
      800
    }

    val deliveryCharge: Future[Int] = Future {
      sleep(150)
      log("Fetched delivery charge")
      40
    }

    val total: Future[Int] =
      price.zip(deliveryCharge).map { case (itemPrice, charge) =>
        itemPrice + charge
      }

    log(s"[B] total amount = ${Await.result(total, 2.seconds)}")
  }

  private def demoAllOf(): Unit = {
    log("\n[C] Future.sequence waits until every future is complete")

    val productIds = List("book", "mouse", "monitor")
    val futures: List[Future[String]] =
      productIds.map { productId =>
        Future {
          sleep(200)
          log(s"Fetched price for $productId")
          s"$productId price = ${productId.length * 100}"
        }
      }

    val prices: Future[List[String]] = Future.sequence(futures)

    log(s"[C] prices: ${Await.result(prices, 2.seconds).mkString(", ")}")
  }

  private def demoExceptionHandling(): Unit = {
    log("\n[D] recover converts a failure into a fallback value")

    val safeResult: Future[String] =
      Future[String] {
        sleep(100)
        throw new IllegalStateException("remote service failed")
      }.recover {
        case error => s"fallback value because: ${error.getMessage}"
      }

    log(s"[D] result: ${Await.result(safeResult, 2.seconds)}")
  }

  private def demoManualCompletion(): Unit = {
    log("\n[E] Promise manually finishes a Future")

    val promise = Promise[String]()
    val future: Future[String] = promise.future

    Future {
      sleep(150)
      promise.success("completed by worker thread")
    }

    log(s"[E] result: ${Await.result(future, 2.seconds)}")
  }

  private def sleep(milliseconds: Long): Unit =
    try Thread.sleep(milliseconds)
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("Interrupted while simulating work")
    }

  private def log(message: String): Unit =
    println(f"${Thread.currentThread().getName}%-20s $message")
}
