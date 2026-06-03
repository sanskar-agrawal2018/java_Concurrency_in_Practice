/**
 * Chapter 7 -- Listing 7.2: Prime Generator Cancellation Demo
 *
 * Demonstrates cooperative cancellation from JCIP Listing 7.2.
 *
 * A PrimeGenerator runs in a background thread generating prime numbers.
 * After one second the main thread calls cancel(). The generator won't
 * necessarily stop after exactly one second -- there is a cooperative
 * delay between when cancel() is called and when the run loop next checks
 * the cancelled flag (Recipient 1 only; no executor pool involved here).
 *
 * Key takeaways:
 *  - Use an AtomicBoolean (volatile equivalent) for the cancellation flag
 *    so that the write from one thread is visible to the reading thread.
 *  - The task must poll the flag regularly; otherwise cancellation is delayed
 *    by the length of one iteration.
 *  - The caller can collect partial results via a thread-safe list even after
 *    cancellation -- work done before the flag was seen is not lost.
 *
 * Run:
 *   sbt "chapter7/runMain PrimeGeneratorCancellationDemo"
 */

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

object PrimeGeneratorCancellationDemo {

  def main(args: Array[String]): Unit = {
    println("=== Listing 7.2: Prime Generator -- run for 1 second then cancel ===")
    println()

    val primes = generatePrimesFor(duration = 1, unit = TimeUnit.SECONDS)

    println()
    println(s"[main] Primes generated in ~1 second : ${primes.size()}")
    println(s"[main] Last few primes                : ${lastN(primes, 10).mkString(", ")}")
    println("[main] Demo complete.")
  }

  /**
   * Starts a PrimeGenerator, lets it run for `duration` units, then
   * requests cancellation and returns whatever primes were collected.
   *
   * This is the core pattern from Listing 7.2: timed run via cooperative
   * cancellation rather than forceful thread interruption.
   */
  private def generatePrimesFor(
      duration: Long,
      unit: TimeUnit
  ): java.util.List[Long] = {

    val generator = new PrimeGenerator
    val thread    = new Thread(generator, "prime-generator")

    thread.start()
    println(s"[main] prime-generator started")

    try {
      unit.sleep(duration)
    } finally {
      // Request cancellation. The generator will see this on its next
      // flag-check and exit -- it may produce a few more primes first.
      println(s"[main] 1 second elapsed -- calling cancel()")
      generator.cancel()
    }

    // Give the thread a moment to notice the flag and finish cleanly.
    thread.join(500)

    if (thread.isAlive) {
      println("[main] WARNING: generator thread did not stop within 500 ms")
    } else {
      println("[main] generator thread has stopped cleanly")
    }

    generator.getPrimes
  }

  private def lastN(list: java.util.List[Long], n: Int): Seq[Long] = {
    val size  = list.size()
    val start = math.max(0, size - n)
    (start until size).map(list.get)
  }
}

/**
 * Generates prime numbers until cancel() is called.
 *
 * The cancelled flag is an AtomicBoolean so that writes from the cancelling
 * thread are immediately visible to the generator thread -- equivalent to
 * declaring a volatile boolean in Java.
 */
final class PrimeGenerator extends Runnable {

  private val cancelled = new AtomicBoolean(false)
  private val primes    = new CopyOnWriteArrayList[Long]()

  override def run(): Unit = {
    var candidate = 2L

    // Poll the cancellation flag on every iteration.
    // When cancel() is called this loop exits after finishing the current
    // primality check -- not necessarily at the exact millisecond requested.
    while (!cancelled.get()) {
      if (isPrime(candidate)) {
        primes.add(candidate)
        if (primes.size() % 5000 == 0)
          println(
            s"[${Thread.currentThread().getName}] " +
            s"${primes.size()} primes found so far, latest=$candidate"
          )
      }
      candidate += 1
    }

    println(
      s"[${Thread.currentThread().getName}] cancellation detected after " +
      s"${primes.size()} primes (last candidate checked: $candidate)"
    )
  }

  /** Request cooperative cancellation. Returns immediately; does not block. */
  def cancel(): Unit = {
    cancelled.set(true)
    println(s"[${Thread.currentThread().getName}] cancel() called -- flag set to true")
  }

  def getPrimes: java.util.List[Long] = primes

  private def isPrime(n: Long): Boolean = {
    if (n < 2) return false
    if (n == 2) return true
    if (n % 2 == 0) return false
    var i = 3L
    while (i * i <= n) {
      if (n % i == 0) return false
      i += 2
    }
    true
  }
}
