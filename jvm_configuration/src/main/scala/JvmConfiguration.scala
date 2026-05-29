/*
 * ============================================================================
 *  JVM CONFIGURATION -- a complete tour of JVM size / memory options
 * ============================================================================
 *
 *  The JVM does NOT use one big "memory pool". At startup it carves the
 *  process's address space into several distinct regions, each with its OWN
 *  size knob. Picking the right size for each region is what JVM tuning is.
 *
 *  Big picture of the JVM process memory layout:
 *
 *      +----------------------------------------------------------+
 *      |  Java HEAP        -- all `new` objects live here         |  -Xms / -Xmx
 *      |    Young Gen (Eden + 2 Survivor)                         |  -Xmn, -XX:NewRatio, -XX:SurvivorRatio
 *      |    Old   Gen                                             |
 *      +----------------------------------------------------------+
 *      |  Metaspace       -- class metadata (native memory!)      |  -XX:MetaspaceSize / -XX:MaxMetaspaceSize
 *      +----------------------------------------------------------+
 *      |  Code Cache      -- JIT-compiled native code             |  -XX:InitialCodeCacheSize / -XX:ReservedCodeCacheSize
 *      +----------------------------------------------------------+
 *      |  Thread Stacks   -- one stack PER thread, native memory  |  -Xss
 *      +----------------------------------------------------------+
 *      |  Direct Memory   -- ByteBuffer.allocateDirect, NIO       |  -XX:MaxDirectMemorySize
 *      +----------------------------------------------------------+
 *      |  GC structures, JNI, internal buffers, mmap'd files ...  |
 *      +----------------------------------------------------------+
 *
 *  The KEY size flags, grouped:
 *
 *  -- Heap ------------------------------------------------------------------
 *    -Xms<size>                    Initial (and minimum) heap. JVM commits this
 *                                  amount up-front.  Example: -Xms512m
 *    -Xmx<size>                    Maximum heap. The JVM will NEVER exceed this;
 *                                  an OutOfMemoryError: Java heap space is
 *                                  thrown when full GC can't reclaim enough.
 *                                  Example: -Xmx2g
 *    -Xmn<size>                    Size of the Young generation. Larger young
 *                                  gen = less frequent but longer minor GCs.
 *    -XX:NewRatio=N                Old / Young size ratio (default 2 → old is
 *                                  2× young).  Mutually exclusive with -Xmn.
 *    -XX:SurvivorRatio=N           Eden / one-survivor ratio (default 8).
 *    -XX:MaxGCPauseMillis=N        Soft goal for GC pause time (G1, ZGC).
 *
 *    Sizes accept suffixes: k/K (KiB), m/M (MiB), g/G (GiB).  Bare numbers are
 *    BYTES, not megabytes -- `-Xmx256` means 256 BYTES and the JVM refuses to
 *    start.  Always use a suffix.
 *
 *  -- Stack -----------------------------------------------------------------
 *    -Xss<size>                    Per-thread stack size.  Default is platform
 *                                  dependent (~512k-1m on 64-bit Linux).
 *                                  Smaller -Xss → more threads fit in the
 *                                  same address space, but deep recursion
 *                                  throws StackOverflowError sooner.
 *
 *  -- Metaspace (replaces PermGen since Java 8) -----------------------------
 *    -XX:MetaspaceSize=<size>      Initial high-water mark that triggers the
 *                                  first class-unloading GC.
 *    -XX:MaxMetaspaceSize=<size>   Hard cap. Default is "unlimited" (i.e.
 *                                  limited only by native RAM).  Hitting it
 *                                  throws OutOfMemoryError: Metaspace.
 *
 *  -- Code cache (JIT) ------------------------------------------------------
 *    -XX:InitialCodeCacheSize=<sz>   Initial reservation for JIT code.
 *    -XX:ReservedCodeCacheSize=<sz>  Maximum.  If it fills up, the JIT stops
 *                                    compiling and the app falls back to the
 *                                    interpreter -- big perf cliff.
 *
 *  -- Direct / off-heap memory ---------------------------------------------
 *    -XX:MaxDirectMemorySize=<sz>  Cap for ByteBuffer.allocateDirect.  Default
 *                                  equals -Xmx, which surprises people.
 *
 *  -- Diagnostic helpers (use ONE of these to inspect a live JVM) ----------
 *    -XX:+PrintFlagsFinal          Print every flag and its effective value at
 *                                  startup.  Useful to see what defaults the
 *                                  JVM chose for the flags you did NOT set.
 *    -Xlog:gc*                     (Java 9+) GC logging.
 *    -XX:+HeapDumpOnOutOfMemoryError   Dump the heap when OOM occurs.
 *    -XX:NativeMemoryTracking=summary  Track non-heap native allocations.
 *
 *  The code below READS each of these regions back at runtime via
 *  java.lang.management and Runtime, and STRESSES each one so you can SEE the
 *  difference between configurations. Run it with different -Xmx / -Xss / etc.
 *  and observe the output change.
 * ============================================================================
 */

import java.lang.management.{ManagementFactory, MemoryPoolMXBean, MemoryType}
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

object JvmConfiguration {

  // ---- pretty-printing helpers ------------------------------------------------

  private def mib(bytes: Long): String =
    if (bytes < 0) "unbounded" else f"${bytes / (1024.0 * 1024.0)}%.2f MiB"

  private def hr(title: String): Unit = {
    val bar = "=" * 78
    println()
    println(bar)
    println(s"  $title")
    println(bar)
  }

  // ---------------------------------------------------------------------------
  // 1. HEAP -- what -Xms / -Xmx actually do.
  //
  //   - Runtime.totalMemory  = currently COMMITTED heap (grows up to -Xmx)
  //   - Runtime.maxMemory    = -Xmx
  //   - Runtime.freeMemory   = free WITHIN totalMemory (not total - used)
  // ---------------------------------------------------------------------------
  private def reportHeap(): Unit = {
    hr("1. HEAP  (-Xms / -Xmx)")
    val rt = Runtime.getRuntime
    val max       = rt.maxMemory()
    val committed = rt.totalMemory()
    val free      = rt.freeMemory()
    val used      = committed - free

    println(f"  -Xmx (max heap)              : ${mib(max)}")
    println(f"  committed heap (≈ -Xms grow) : ${mib(committed)}")
    println(f"  used heap                    : ${mib(used)}")
    println(f"  free within committed        : ${mib(free)}")

    // Break the heap up into its named pools (Eden, Survivor, Old, ...).
    val heapPools: List[MemoryPoolMXBean] =
      ManagementFactory.getMemoryPoolMXBeans.asScala
        .filter(_.getType == MemoryType.HEAP).toList

    println("  heap pools (generations):")
    heapPools.foreach { p =>
      val u = p.getUsage
      println(f"    ${p.getName}%-28s used=${mib(u.getUsed)}%-14s committed=${mib(u.getCommitted)}%-14s max=${mib(u.getMax)}")
    }
  }

  /** Allocate ~80% of the max heap to see committed memory grow toward -Xmx. */
  private def stressHeap(): Unit = {
    hr("1b. HEAP STRESS  -- watch committed grow toward -Xmx")
    val rt = Runtime.getRuntime
    val target = (rt.maxMemory() * 0.8).toLong
    val chunks = scala.collection.mutable.ListBuffer.empty[Array[Byte]]
    val chunk  = 1 * 1024 * 1024 // 1 MiB
    try {
      var allocated = 0L
      while (allocated < target) {
        chunks += new Array[Byte](chunk)
        allocated += chunk
      }
      println(f"  allocated ${mib(allocated)} of objects without hitting OOM")
    } catch {
      case _: OutOfMemoryError =>
        println("  hit OutOfMemoryError: Java heap space  (raise -Xmx)")
    } finally {
      chunks.clear()
      System.gc()
    }
    reportHeap()
  }

  // ---------------------------------------------------------------------------
  // 2. STACK -- -Xss controls per-thread stack size.
  //
  // Each Java stack frame is roughly tens of bytes plus its locals.  A
  // recursive method gives us a cheap way to measure depth-vs-stack-size.
  // ---------------------------------------------------------------------------
  private def measureStackDepth(): Int = {
    var depth = 0
    def recurse(): Unit = {
      depth += 1
      recurse()
    }
    try recurse() catch { case _: StackOverflowError => /* expected */ }
    depth
  }

  private def reportStack(): Unit = {
    hr("2. STACK  (-Xss)")
    // The JVM exposes the running thread count and peak but NOT the -Xss value
    // directly via a stable API; we derive it from HotSpot diagnostic flags.
    val xss = readHotSpotFlag("ThreadStackSize")  // value is in KiB
    println(s"  -Xss (from HotSpot flag ThreadStackSize): ${xss}k per thread")

    val depth = measureStackDepth()
    println(f"  recursion depth before StackOverflowError: $depth%,d frames")
    println("  (lower -Xss → fewer frames; higher -Xss → more, but each thread costs more native memory)")

    val tmx = ManagementFactory.getThreadMXBean
    println(f"  live threads                : ${tmx.getThreadCount}")
    println(f"  peak threads since start    : ${tmx.getPeakThreadCount}")
  }

  // ---------------------------------------------------------------------------
  // 3. METASPACE -- class metadata.  Loaded classes cost metaspace, NOT heap.
  // ---------------------------------------------------------------------------
  private def reportMetaspace(): Unit = {
    hr("3. METASPACE  (-XX:MetaspaceSize / -XX:MaxMetaspaceSize)")
    val pools = ManagementFactory.getMemoryPoolMXBeans.asScala
      .filter(p => p.getName.toLowerCase.contains("metaspace") ||
                   p.getName.toLowerCase.contains("class space"))
      .toList
    pools.foreach { p =>
      val u = p.getUsage
      println(f"  ${p.getName}%-32s used=${mib(u.getUsed)}%-14s committed=${mib(u.getCommitted)}%-14s max=${mib(u.getMax)}")
    }
    val cl = ManagementFactory.getClassLoadingMXBean
    println(f"  classes currently loaded    : ${cl.getLoadedClassCount}")
    println(f"  classes loaded since start  : ${cl.getTotalLoadedClassCount}")
    println(f"  classes unloaded since start: ${cl.getUnloadedClassCount}")
  }

  // ---------------------------------------------------------------------------
  // 4. CODE CACHE -- where JIT-compiled native methods live.
  // ---------------------------------------------------------------------------
  private def reportCodeCache(): Unit = {
    hr("4. CODE CACHE  (-XX:ReservedCodeCacheSize)")
    val pools = ManagementFactory.getMemoryPoolMXBeans.asScala
      .filter(_.getName.toLowerCase.contains("code"))
      .toList
    if (pools.isEmpty) println("  (no code-cache pools exposed by this JVM)")
    pools.foreach { p =>
      val u = p.getUsage
      println(f"  ${p.getName}%-32s used=${mib(u.getUsed)}%-14s committed=${mib(u.getCommitted)}%-14s max=${mib(u.getMax)}")
    }
    val comp = ManagementFactory.getCompilationMXBean
    if (comp != null)
      println(f"  total JIT compile time      : ${comp.getTotalCompilationTime} ms (${comp.getName})")
  }

  // ---------------------------------------------------------------------------
  // 5. DIRECT MEMORY -- ByteBuffer.allocateDirect lives OUTSIDE the heap.
  //   Capped by -XX:MaxDirectMemorySize.  Default = -Xmx, which is a footgun.
  // ---------------------------------------------------------------------------
  private def reportDirectMemory(): Unit = {
    hr("5. DIRECT MEMORY  (-XX:MaxDirectMemorySize)")
    val maxDirect = readHotSpotFlag("MaxDirectMemorySize") // bytes; 0 = default(=-Xmx)
    val resolved =
      if (maxDirect == 0) Runtime.getRuntime.maxMemory() else maxDirect
    println(f"  -XX:MaxDirectMemorySize     : ${mib(resolved)}  (0 means 'same as -Xmx')")

    // Allocate a small direct buffer to prove the path works.
    val buf: ByteBuffer = ByteBuffer.allocateDirect(8 * 1024 * 1024) // 8 MiB
    println(f"  allocated a direct ByteBuffer: capacity=${mib(buf.capacity().toLong)}")

    // The official off-heap pool MXBeans show this allocation:
    ManagementFactory.getPlatformMXBeans(classOf[java.lang.management.BufferPoolMXBean])
      .asScala.foreach { bp =>
        println(f"  BufferPool ${bp.getName}%-12s count=${bp.getCount}%-6d  used=${mib(bp.getMemoryUsed)}  capacity=${mib(bp.getTotalCapacity)}")
      }
  }

  // ---------------------------------------------------------------------------
  // 6. GC -- a configuration choice as important as size.  Print which
  //    collector is in use plus per-collector counts and total pause time.
  // ---------------------------------------------------------------------------
  private def reportGc(): Unit = {
    hr("6. GARBAGE COLLECTOR  (-XX:+UseG1GC / -XX:+UseZGC / -XX:+UseParallelGC ...)")
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.asScala
    gcs.foreach { gc =>
      println(f"  ${gc.getName}%-28s collections=${gc.getCollectionCount}%-6d  total pause=${gc.getCollectionTime}%5d ms")
      println(s"    operates on pools: ${gc.getMemoryPoolNames.mkString(", ")}")
    }
  }

  // ---------------------------------------------------------------------------
  // 7. THE COMMAND LINE THE JVM WAS GIVEN -- shows exactly which size flags
  //    are active so the rest of the output makes sense.
  // ---------------------------------------------------------------------------
  private def reportRuntime(): Unit = {
    hr("0. JVM INVOCATION")
    val rt = ManagementFactory.getRuntimeMXBean
    println(s"  VM name / vendor : ${rt.getVmName} / ${rt.getVmVendor}")
    println(s"  VM version       : ${rt.getVmVersion}")
    println(s"  uptime           : ${rt.getUptime} ms")
    println(s"  input arguments  : ${rt.getInputArguments.asScala.mkString(" ")}")
    val os = ManagementFactory.getOperatingSystemMXBean
    println(s"  available CPUs   : ${os.getAvailableProcessors}")
  }

  // ---------------------------------------------------------------------------
  // Read a HotSpot diagnostic flag (works on the HotSpot/OpenJDK JVM).  We use
  // this to recover values like ThreadStackSize that have no first-class API.
  // ---------------------------------------------------------------------------
  private def readHotSpotFlag(name: String): Long = {
    try {
      val server = ManagementFactory.getPlatformMBeanServer
      val on     = new javax.management.ObjectName("com.sun.management:type=HotSpotDiagnostic")
      val vm     = javax.management.JMX.newMXBeanProxy(
        server, on, classOf[com.sun.management.HotSpotDiagnosticMXBean])
      vm.getVMOption(name).getValue.toLong
    } catch {
      case _: Throwable => -1L
    }
  }

  // ---------------------------------------------------------------------------
  // Continuous monitor: print a one-line snapshot every `intervalMs` until the
  // process is killed.  Handy when you want to WATCH the heap breathe under
  // load.  Call from main with arg "monitor".
  // ---------------------------------------------------------------------------
  private def monitorLoop(intervalMs: Long): Unit = {
    val rt  = Runtime.getRuntime
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.asScala.toList
    println(f"  time(s)  heapUsed     heapCommit   heapMax       gcCount  gcPause(ms)")
    val start = System.nanoTime()
    while (true) {
      val committed = rt.totalMemory()
      val free      = rt.freeMemory()
      val used      = committed - free
      val max       = rt.maxMemory()
      val (count, pause) = gcs.foldLeft((0L, 0L)) { case ((c, p), gc) =>
        (c + gc.getCollectionCount, p + gc.getCollectionTime)
      }
      val t = (System.nanoTime() - start) / 1e9
      println(f"  $t%6.1f   ${mib(used)}%-12s ${mib(committed)}%-12s ${mib(max)}%-12s $count%-7d  $pause")
      Thread.sleep(intervalMs)
    }
  }

  // ---------------------------------------------------------------------------
  // Entry point.
  //   no args                -> single-shot report
  //   "monitor [intervalMs]" -> keep printing a snapshot forever
  //   "stress"               -> single-shot report THEN allocate to ~80% heap
  // ---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    reportRuntime()
    reportHeap()
    reportStack()
    reportMetaspace()
    reportCodeCache()
    reportDirectMemory()
    reportGc()

    args.toList match {
      case "stress" :: _ =>
        stressHeap()
      case "monitor" :: rest =>
        val interval = rest.headOption.flatMap(_.toLongOption).getOrElse(1000L)
        hr(s"LIVE MONITOR (every ${interval} ms; Ctrl+C to stop)")
        monitorLoop(interval)
      case _ =>
        println()
        println("  (pass `stress` to fill the heap, or `monitor [ms]` for a live view)")
    }
  }
}