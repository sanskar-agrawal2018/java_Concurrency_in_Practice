import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}

object FuturesAsync {

  // Demo 1: simple Scala Future with combinators and blocking wait for demo purposes
  private def demoScalaFuture(): Unit = {
    println("[ScalaFuture] starting demo")
    val f: Future[Int] = Future {
      println(s"[ScalaFuture][${Thread.currentThread().getName}] computing value...")
      Thread.sleep(300)
      21 * 2
    }

    val mapped: Future[String] = f.map(v => s"result=$v")

    mapped.onComplete { either =>
      println(s"[ScalaFuture][${Thread.currentThread().getName}] onComplete: $either")
    }

    // For demo only: block briefly to show the result (avoid blocking in real apps)
    val r = Await.result(mapped, 1.second)
    println(s"[ScalaFuture][${Thread.currentThread().getName}] Await.result(mapped) = $r")
  }

  // Demo 2: Promise that we complete from another async task
  private def demoPromise(): Unit = {
    println(s"[Promise][${Thread.currentThread().getName}] starting demo")
    val p = Promise[String]()
    val fut = p.future

    // Consumer: reacts when promise is completed
    fut.onComplete { res => println(s"[Promise] consumer got: $res") }

    // Producer: complete the promise asynchronously
    Future {
      Thread.sleep(200)
      p.success("completed-by-producer")
    }

    // Wait for the promise to complete (demo only)
    println(s"[Promise] Await: ${Await.result(fut, 1.second)}")
  }

  // Demo 3: Java CompletableFuture with async composition
  private def demoJavaCompletableFuture(): Unit = {
    println("\n[CompletableFuture] starting demo")
    val exec = Executors.newFixedThreadPool(2)

    try {
      val cf: CompletableFuture[String] = CompletableFuture.supplyAsync(() => {
        Thread.sleep(250)
        "hello"
      }, exec)

      val composed = cf.thenApply((s: String) => s.toUpperCase())
        .thenCompose((s: String) => CompletableFuture.supplyAsync(() => s + " world", exec))

      composed.thenAccept((s: String) => println(s"[CompletableFuture] final = $s"))

      // block to observe result in demo
      composed.get(1, TimeUnit.SECONDS)
    } finally {
      exec.shutdown()
    }
  }

  def main(args: Array[String]): Unit = {
    println(s"[main] Main thread: ${Thread.currentThread().getName}")
    demoScalaFuture()
    demoPromise()
    demoJavaCompletableFuture()
  }
}

