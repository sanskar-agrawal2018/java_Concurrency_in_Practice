import com.sun.jna.{Library, Native, Platform}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

// ─── JNA bridge to Linux scheduler syscalls ────────────────────────────────
object CLib {
  trait CLibrary extends Library {
    def sched_getcpu(): Int                          // which core am I on right now?
    def sched_setaffinity(pid: Int, cpusetsize: Int, mask: Array[Byte]): Int
  }

  val instance: CLibrary =
    Native.load("c", classOf[CLibrary])

  /** Pin the entire JVM process to a single logical core (0-indexed). */
  def pinToCore(coreIndex: Int): Unit = {
    val cpuSetSize = 128  // kernel cpu_set_t is 1024 bits = 128 bytes
    val mask = new Array[Byte](cpuSetSize)
    // Set bit at position coreIndex
    mask(coreIndex / 8) = (1 << (coreIndex % 8)).toByte
    val ret = instance.sched_setaffinity(0, cpuSetSize, mask)
    if (ret != 0) throw new RuntimeException(s"sched_setaffinity failed: $ret")
    println(s"[INIT] JVM process pinned to CPU core $coreIndex\n")
  }

  def currentCore(): Int = instance.sched_getcpu()
}

// ─── Per-thread core monitor ────────────────────────────────────────────────
/**
 * Runs in a background daemon thread.
 * Continuously polls sched_getcpu() and fires callbacks when the monitored
 * thread LEAVES or RE-ENTERS the core (its core id changes / comes back).
 */
class CoreMonitor(watchedThread: Thread) extends Runnable {
  @volatile private var running = true
  private val NO_CORE = -1
  private var lastCore = NO_CORE

  def stop(): Unit = running = false

  override def run(): Unit = {
    // We can't call sched_getcpu on behalf of another thread directly,
    // so we ask the watched thread to sample itself via a shared field.
    // Instead, we poll thread state + shared core field set by the task itself.
    while (running) {
      Thread.sleep(5)  // 5 ms polling interval
    }
  }
}

// ─── The actual task run by worker threads ──────────────────────────────────
class TrackedTask(taskId: Int, waitMillis: Long) extends Runnable {

  private def tname = Thread.currentThread().getName
  private def core  = CLib.currentCore()

  override def run(): Unit = {
    // ── Phase 1: Active work ─────────────────────────────────────────────
    val coreAtStart = core
    println(s"[ENTER ] task-$taskId | thread=$tname | core=$coreAtStart | starting work")

    // Simulate CPU work
    var x = 0L
    for (_ <- 1 to 500_000) x += math.sqrt(x + 1).toLong

    // ── Phase 2: Enter WAITING state ─────────────────────────────────────
    val coreBeforeSleep = core
    println(
      s"[LEAVE ] task-$taskId | thread=$tname | core=$coreBeforeSleep | " +
        s"entering wait (sleep ${waitMillis}ms) — core $coreBeforeSleep is now FREE"
    )

    Thread.sleep(waitMillis)   // <-- thread WAITS; OS scheduler picks another runnable thread

    // ── Phase 3: Woken up — check which core we're on now ────────────────
    val coreAfterSleep = core
    val coreChanged = if (coreAfterSleep != coreBeforeSleep) "DIFFERENT CORE!" else "same core"
    println(
      s"[RESUME] task-$taskId | thread=$tname | core=$coreAfterSleep | " +
        s"woke up ($coreChanged vs before-sleep=$coreBeforeSleep)"
    )

    // ── Phase 4: More work after wake ────────────────────────────────────
    for (_ <- 1 to 200_000) x += math.sqrt(x + 1).toLong
    println(s"[DONE  ] task-$taskId | thread=$tname | core=$core | finished")
  }
}

// ─── Companion that also watches which thread owns the core globally ────────
object CoreOwnerPoller extends Runnable {
  // Each worker thread sets this to its name while running, clears on wait

  val currentOwner = new java.util.concurrent.atomic.AtomicReference[String]("none")

  @volatile var running = true

  override def run(): Unit = {
    println(s"[CORE-POLL][${Thread.currentThread().getName}][CoreName = ] Starting core ownership poller daemon thread")
    var last = ""
    while (running) {
      val now = currentOwner.get()
      if (now != last) {
        val c = CLib.currentCore()
        println(s"  *** [CORE-POLL] core=$c is now owned by: $now ***")
        last = now
      }
      Thread.sleep(10)
    }
  }
}

// ─── Main ───────────────────────────────────────────────────────────────────
object CoreWatch extends App {
  // Require Linux
  println(s"[ENTER] task-main | thread=${Thread.currentThread().getName} | Running on core = ${CLib.currentCore()}")

  require(Platform.isLinux, "sched_setaffinity is Linux-only")

  // Pin to core 0
  CLib.pinToCore(0)

  println(s"Current core: ${CLib.currentCore()}")

  // Start global core-owner poller daemon
  val pollerThread = new Thread(CoreOwnerPoller, "core-poller")
  pollerThread.setDaemon(true)
  pollerThread.start()

  // Fixed thread pool of 3 threads — but only 1 core available!
  val pool = Executors.newFixedThreadPool(3)

  // Submit 6 tasks with varying wait times
  for (i <- 1 to 6) {
    val waitMs = 50 + (i * 30L)   // 80ms, 110ms, 140ms … each task waits longer
    pool.submit(new TrackedTask(i, waitMs))
    println(s"[LEAVE] task-main | thread=${Thread.currentThread().getName} | Running on core = ${CLib.currentCore()}")
    Thread.sleep(20)               // stagger submission slightly
    println(s"[ENTER] task-main | thread=${Thread.currentThread().getName} | Running on core = ${CLib.currentCore()}")

  }

  pool.shutdown()
  pool.awaitTermination(30, TimeUnit.SECONDS)
  CoreOwnerPoller.running = false

  println("\n[DONE] All tasks finished.")
}