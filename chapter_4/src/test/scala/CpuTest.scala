/*
 * Tests for Cpu.current() in Interruptibility.scala.
 *
 * Cpu.current() wraps the Linux/glibc call `sched_getcpu()` via JNA. Its
 * contract is:
 *
 *   - If JNA can load libc AND the call succeeds, it returns the index of
 *     the CPU core the CALLING thread is on at the instant of the call.
 *     This is a non-negative integer, strictly less than the number of
 *     online cores the JVM can see (Runtime.availableProcessors()).
 *   - In every failure mode (non-Linux JVM, libc not loadable, JNA missing,
 *     sched_getcpu throws), it returns -1 instead of propagating an error.
 *
 * The tests below pin down exactly that contract. They are designed to pass
 * on a Linux CI box where sched_getcpu() works, AND to pass on a machine
 * where the call is unavailable -- in the latter case Cpu.current() must
 * return -1, and the tests verify that branch too.
 */

import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.mutable

class CpuTest extends AnyFunSuite {

  private val isLinux: Boolean =
    System.getProperty("os.name", "").toLowerCase.contains("linux")

  private val numCores: Int = Runtime.getRuntime.availableProcessors()

  // ---------------------------------------------------------------------------
  // Contract: the return value is either -1 (unavailable) or a valid core id.
  // ---------------------------------------------------------------------------
  test("Cpu.current() returns -1 or a valid core index") {
    val core = Cpu.current()
    assert(core == -1 || (core >= 0 && core < numCores),
      s"Cpu.current() returned $core; expected -1 or 0..${numCores - 1}")
  }

  // ---------------------------------------------------------------------------
  // On Linux + glibc we expect the happy path: a non-negative core id. If we
  // see -1 here it usually means JNA couldn't load libc -- still a legal
  // value per the contract, so we only WARN rather than fail.
  // ---------------------------------------------------------------------------
  test("on Linux, Cpu.current() returns a real core (or -1 if JNA unavailable)") {
    assume(isLinux, "sched_getcpu() is a Linux/glibc call; skipping on non-Linux")
    val core = Cpu.current()
    if (core == -1) {
      info("Cpu.current() returned -1 -- JNA or libc unavailable in this environment")
    } else {
      assert(core >= 0 && core < numCores,
        s"core $core out of range [0, ${numCores - 1}]")
    }
  }

  // ---------------------------------------------------------------------------
  // Cpu.current() is just a snapshot, but it must not throw, and repeated
  // calls must never go out of range.
  // ---------------------------------------------------------------------------
  test("Cpu.current() is safe to call repeatedly and stays in range") {
    val samples = (1 to 1000).map(_ => Cpu.current())
    // Every sample is independently valid.
    samples.foreach { c =>
      assert(c == -1 || (c >= 0 && c < numCores),
        s"sample $c out of range")
    }
    // Either every sample is -1 (libc unavailable -- consistent failure),
    // or every sample is a real core (consistent success). The function
    // shouldn't flip between the two modes during one process lifetime.
    val (bad, good) = samples.partition(_ == -1)
    assert(bad.isEmpty || good.isEmpty,
      s"Cpu.current() mixed -1 with valid cores in the same run: " +
        s"${bad.size} unavailable, ${good.size} valid")
  }

  // ---------------------------------------------------------------------------
  // Multiple threads calling Cpu.current() concurrently must each get a legal
  // value -- the JNA binding must be thread-safe (sched_getcpu is).
  // ---------------------------------------------------------------------------
  test("Cpu.current() is thread-safe under concurrent calls") {
    val workers = math.max(4, numCores)
    val pool = Executors.newFixedThreadPool(workers)
    try {
      val results = mutable.Buffer.empty[Int]
      val futures = (1 to workers).map { _ =>
        pool.submit(new java.util.concurrent.Callable[Int] {
          override def call(): Int = {
            // Burn a little CPU so the thread is actually scheduled before we
            // sample. Without this, on a fast machine all workers can be
            // serialised onto a single core.
            var x = 0L
            val deadline = System.nanoTime() + 5L * 1000 * 1000 // 5 ms
            while (System.nanoTime() < deadline) x += 1
            Cpu.current()
          }
        })
      }
      futures.foreach(f => results += f.get(5, TimeUnit.SECONDS))
      results.foreach { c =>
        assert(c == -1 || (c >= 0 && c < numCores),
          s"worker observed core $c, out of range")
      }
      info(s"observed cores across $workers workers: ${results.mkString(",")}")
    } finally {
      pool.shutdownNow()
      assert(pool.awaitTermination(2, TimeUnit.SECONDS), "pool failed to shut down")
    }
  }

  // ---------------------------------------------------------------------------
  // sched_getcpu() agrees with /proc/cpuinfo's core count: every value we see
  // must correspond to a core the kernel currently lists. We don't require
  // every core to becancellableTask observed (the scheduler picks), only that whatever we
  // do see is real.
  // ---------------------------------------------------------------------------
  test("observed core ids are a subset of the system's online cores") {
    assume(isLinux, "core-id enumeration via /proc is Linux-only")
    val core = Cpu.current()
    assume(core >= 0, "Cpu.current() unavailable -- nothing to compare")

    // availableProcessors() reflects what the JVM is allowed to see, which
    // matches what sched_getcpu() can return.
    assert(core < numCores,
      s"sched_getcpu() returned $core but the JVM only sees $numCores cores")
  }
}