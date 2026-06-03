/**
 * Scala try / catch / finally demo
 *
 * Covers:
 *  1. Basic try-catch-finally
 *  2. Catching multiple exception types
 *  3. scala.util.Try -- functional error handling (no thrown exceptions)
 *  4. Nested try blocks
 *  5. Custom exceptions
 */

import scala.util.{Failure, Success, Try}

// ── Custom exception hierarchy ────────────────────────────────────────────────
class AppException(msg: String)         extends Exception(msg)
class ValidationException(msg: String)  extends AppException(msg)
class DatabaseException(msg: String)    extends AppException(msg)

object TryCatchDemo {
  def main(args: Array[String]): Unit = {
    basicTryCatch()
    println()
    multipleExceptions()
    println()
    functionalTry()
    println()
    nestedTry()
    println()
    customExceptions()
  }

  // ── 1. Basic try / catch / finally ─────────────────────────────────────────
  private def basicTryCatch(): Unit = {
    println("=== 1. Basic try-catch-finally ===")

    // finally always runs -- even when an exception is thrown
    try {
      val result = 10 / 0          // ArithmeticException
      println(s"result: $result")  // never reached
    } catch {
      case e: ArithmeticException =>
        println(s"caught ArithmeticException: ${e.getMessage}")
        return
      case e:Exception =>
          println(s"Unknown exception: ${e.getMessage}")
    } finally {
      println("finally block always runs (cleanup, close resources, etc.)")
    }
    println("execution continues after try-catch-finally")
  }

  // ── 2. Multiple exception types ────────────────────────────────────────────
  private def multipleExceptions(): Unit = {
    println("=== 2. Catching multiple exception types ===")

    def riskyParse(s: String): Int = s.toInt   // throws NumberFormatException
    def riskyIndex(arr: Array[Int], i: Int): Int = arr(i) // throws AIOOBE

    val inputs = List(("42", 0), ("bad", 0), ("1", 99))

    inputs.foreach { case (str, idx) =>
      try {
        val n   = riskyParse(str)
        val arr = Array(  n, n * 2, n * 3)
        println(s"arr[$idx] = ${riskyIndex(arr, idx)}")
      } catch {
        case _: NumberFormatException =>
          println(s"'$str' is not a valid integer")
          return
         // early return from the method on this error
        case e: ArrayIndexOutOfBoundsException =>
          println(s"index $idx out of bounds: ${e.getMessage}")
          return
        case e: Exception =>
          println(s"unexpected error: $e") // catch-all safety net

      }
      finally {
        println("finished processing input\n")
      }

    }
  }

  // ── 3. scala.util.Try -- functional style ──────────────────────────────────
  //
  // Try[A] is either Success(value) or Failure(exception).
  // It lets you treat errors as values and chain transformations safely
  // without scattering try-catch blocks everywhere.
  private def functionalTry(): Unit = {
    println("=== 3. scala.util.Try (functional error handling) ===")

    def divide(a: Int, b: Int): Try[Int] = Try(a / b)

    // map and flatMap only run on Success; Failure short-circuits
    val result1 = divide(10, 2).map(_ * 3)          // Success(15)
    val result2 = divide(10, 0).map(_ * 3)           // Failure(ArithmeticException)
    val result3 = divide(10, 2).flatMap(divide(_, 0)) // Failure (inner fails)

    List(result1, result2, result3).foreach {
      case Success(v) => println(s"Success: $v")
      case Failure(e) => println(s"Failure: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    // getOrElse provides a default when the computation fails
    val safe = divide(10, 0).getOrElse(-1)
    println(s"getOrElse default on failure: $safe")

    // recover turns a known Failure back into a Success
    val recovered = divide(10, 0).recover {
      case _: ArithmeticException => 0
    }
    println(s"recovered value: $recovered")
  }

  // ── 4. Nested try blocks ───────────────────────────────────────────────────
  private def nestedTry(): Unit = {
    println("=== 4. Nested try blocks ===")

    try {
      println("outer try: starting")
      try {
        println("inner try: about to fail")
        throw new IllegalStateException("inner failure")
      } catch {
        case e: IllegalStateException =>
          println(s"inner catch: handled '${e.getMessage}'; rethrowing as AppException")
          throw new AppException(s"wrapped: ${e.getMessage}")
      } finally {
        println("inner finally: always runs")
      }
    } catch {
      case e: AppException =>
        println(s"outer catch: AppException -- ${e.getMessage}")
    } finally {
      println("outer finally: always runs")
    }
  }

  // ── 5. Custom exceptions ───────────────────────────────────────────────────
  private def customExceptions(): Unit = {
    println("=== 5. Custom exception hierarchy ===")

    def validateAge(age: Int): Unit =
      if (age < 0 || age > 150) throw new ValidationException(s"invalid age: $age")

    def saveToDb(name: String): Unit =
      if (name.isEmpty) throw new DatabaseException("name cannot be empty")

    val inputs = List(("Alice", 30), ("", 25), ("Bob", -5))

    inputs.foreach { case (name, age) =>
      try {
        validateAge(age)
        saveToDb(name)
        println(s"saved ($name, $age) successfully")
      } catch {
        case e: ValidationException => println(s"validation error: ${e.getMessage}")
        case e: DatabaseException   => println(s"database error:   ${e.getMessage}")
        case e: AppException        => println(s"app error:        ${e.getMessage}")
      }
    }
  }
}
