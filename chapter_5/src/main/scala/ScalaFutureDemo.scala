import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ScalaFutureDemo {
  private implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  def main(args: Array[String]): Unit = {
    try {
      demoCreateMapAndCallback()
      demoFlatMapAndForComprehension()
      demoParallelZip()
      demoSequenceAndTraverse()
      demoRecover()
    } finally {
      ec.shutdown()
    }
  }

  private def demoCreateMapAndCallback(): Unit = {
    log("\n[A] Future creation + map + onComplete")

    val number: Future[Int] = Future {
      sleep(200)
      log("computed number")
      21
    }

    val doubled: Future[Int] = number.map(value => value * 2)

    doubled.onComplete {
      case Success(value) => log(s"callback received success: $value")
      case Failure(error) => log(s"callback received failure: ${error.getMessage}")
    }

    log("main thread continues while Future runs")
    log(s"[A] result from Await = ${Await.result(doubled, 2.seconds)}")
  }

  private def demoFlatMapAndForComprehension(): Unit = {
    log("\n[B] flatMap and for-comprehension for dependent async work")

    val profileWithFlatMap: Future[String] =
      fetchUserName(7).flatMap { userName =>
        fetchLastOrder(userName).map { order =>
          s"$userName last order = $order"
        }
      }

    log(s"[B] flatMap result = ${Await.result(profileWithFlatMap, 2.seconds)}")

    val profileWithFor: Future[String] =
      for {
        userName <- fetchUserName(8)
        order <- fetchLastOrder(userName)
      } yield s"$userName last order = $order"

    log(s"[B] for-comprehension result = ${Await.result(profileWithFor, 2.seconds)}")
  }

  private def demoParallelZip(): Unit = {
    log("\n[C] zip combines two independent Futures")

    val itemPrice: Future[Int] = Future {
      sleep(250)
      log("fetched item price")
      800
    }

    val deliveryCharge: Future[Int] = Future {
      sleep(150)
      log("fetched delivery charge")
      40
    }

    val total: Future[Int] =
      itemPrice.zip(deliveryCharge).map { case (price, charge) => price + charge }

    log(s"[C] total amount = ${Await.result(total, 2.seconds)}")
  }

  private def demoSequenceAndTraverse(): Unit = {
    log("\n[D] sequence and traverse collect many Futures")

    val ids = List("book", "mouse", "monitor")

    val futures: List[Future[String]] = ids.map(fetchProductPrice)
    val pricesFromSequence: Future[List[String]] = Future.sequence(futures)

    log(s"[D] sequence result = ${Await.result(pricesFromSequence, 2.seconds).mkString(", ")}")

    val pricesFromTraverse: Future[List[String]] =
      Future.traverse(ids)(fetchProductPrice)

    log(s"[D] traverse result = ${Await.result(pricesFromTraverse, 2.seconds).mkString(", ")}")
  }

  private def demoRecover(): Unit = {
    log("\n[E] recover turns a failed Future into a fallback value")

    val failedFuture: Future[String] = Future {
      sleep(100)
      throw new IllegalStateException("remote service failed")
    }

    val safeFuture: Future[String] =
      failedFuture.recover {
        case error => s"fallback value because: ${error.getMessage}"
      }

    log(s"[E] result = ${Await.result(safeFuture, 2.seconds)}")
  }

  private def fetchUserName(userId: Int): Future[String] = Future {
    sleep(150)
    log(s"fetched user for id $userId")
    s"user-$userId"
  }

  private def fetchLastOrder(userName: String): Future[String] = Future {
    sleep(150)
    log(s"fetched last order for $userName")
    "keyboard"
  }

  private def fetchProductPrice(productId: String): Future[String] = Future {
    sleep(150)
    log(s"fetched price for $productId")
    s"$productId price = ${productId.length * 100}"
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
