/*
 * Desktop Search Example  (Java Concurrency in Practice, Listings 5.8 - 5.9)
 *
 * This is the classic PRODUCER-CONSUMER pattern built on a BlockingQueue.
 *
 *   Producers : FileCrawler threads walk the directory tree and PUT every
 *               file they discover onto a shared queue.
 *   Consumer  : Indexer threads TAKE files off that queue and "index" them.
 *
 * The BlockingQueue is the only point of contact between the two sides, and
 * it does all the hard concurrency work for us:
 *
 *   - put(...)  blocks the producer when the queue is FULL  (back-pressure:
 *               crawlers cannot race ahead and exhaust memory).
 *   - take()    blocks the consumer when the queue is EMPTY (the indexer
 *               sleeps instead of busy-waiting until work arrives).
 *
 * Because the queue is thread-safe, producers and consumers never touch each
 * other's state directly -- no shared mutable data, no manual locking. The
 * two activities are also DECOUPLED: crawling and indexing run at their own
 * pace, and we cestan add more threads to whichever side is the bottleneck.
 */

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

// ---------------------------------------------------------------------------
// PRODUCER: FileCrawler
//
// One crawler is responsible for one root directory. It recursively descends
// the tree and publishes each file it finds onto the shared queue.
// ---------------------------------------------------------------------------
class FileCrawler(
  fileQueue: BlockingQueue[File],
  fileFilter: java.io.FileFilter,
  root: File
) extends Runnable {

  // run() is the thread's entry point. We catch InterruptedException so that,
  // when someone asks this thread to stop, we exit the loop cleanly AND
  // restore the interrupt flag (Thread.currentThread().interrupt()) so callers
  // higher up the stack can still observe that an interruption happened.
  def run(): Unit =
    try crawl(root)
    catch { case _: InterruptedException => Thread.currentThread().interrupt() }

  private def crawl(root: File): Unit = {
    // listFiles returns null on an I/O error or if `root` is not a directory,
    // hence the explicit null check below.
    val entries = root.listFiles(fileFilter)
    if (entries != null) entries.foreach { entry =>
      if (entry.isDirectory) crawl(entry)        // recurse into sub-directories
      else if (!alreadyIndexed(entry))           // skip files we already have
        fileQueue.put(entry)                     // BLOCKS if the queue is full
    }
  }

  // Placeholder for "have we seen this file before?". A real desktop search
  // would consult an on-disk index here; for the demo nothing is indexed yet,
  // so every file is treated as new.
  private def alreadyIndexed(file: File): Boolean = false
}

// ---------------------------------------------------------------------------
// CONSUMER: Indexer
//
// Each indexer loops forever, pulling one file at a time off the queue and
// indexing it. take() parks the thread while the queue is empty, so an idle
// indexer consumes no CPU.
// ---------------------------------------------------------------------------
class Indexer(queue: BlockingQueue[File]) extends Runnable {

  def run(): Unit =
    try while (true) indexFile(queue.take())     // take() BLOCKS while empty
    catch { case _: InterruptedException => Thread.currentThread().interrupt() }

  // Placeholder for the real indexing work (parsing the file, updating a
  // search index, ...). Here we just report what was consumed and from which
  // thread, which makes the producer/consumer hand-off visible at runtime.
  private def indexFile(file: File): Unit =
    println(s"[${Thread.currentThread().getName}] indexing ${file.getPath}")
}

// ---------------------------------------------------------------------------
// Wiring + demo.
//
// startIndexing creates the shared queue, launches one crawler per root and
// `nConsumers` indexers. After this returns, all threads are running and the
// queue is shuttling files from the producers to the consumers.
// ---------------------------------------------------------------------------
object DesktopSearch {

  // Maximum number of files allowed to sit in the queue at once. A bounded
  // queue is what gives us back-pressure: once `bound` files are waiting,
  // crawlers block on put(...) until an indexer drains one.
  private val bound      = 100

  // How many indexer (consumer) threads to run in parallel.
  private val nConsumers = 4

  def startIndexing(roots: Array[File]): Unit = {
    val queue = new LinkedBlockingQueue[File](bound)
    // One producer thread per root directory. `_ => true` is a FileFilter
    // that accepts everything (a real app might filter by extension).
    roots.foreach(root => new Thread(new FileCrawler(queue, _ => true, root)).start())
    // A pool of consumer threads, all sharing the one queue.
    (0 until nConsumers).foreach(_ => new Thread(new Indexer(queue)).start())
  }

  def main(args: Array[String]): Unit = {
    // Crawl this project directory by default, or any paths passed on the CLI.
    val roots =
      if (args.nonEmpty) args.map(new File(_))
      else Array(new File("."))

    println(s"Starting desktop search over: ${roots.map(_.getPath).mkString(", ")}")
    startIndexing(roots)

    // The crawler/indexer threads above are non-daemon and the indexers loop
    // forever on take(), so the JVM would never exit on its own. For this demo
    // we simply let indexing run for a short while, then stop the process.
    Thread.sleep(2000)
    println("Demo finished -- stopping.")
    System.exit(0)
  }
}

// ===========================================================================
// TESTS
//
// These tests are written as a plain, dependency-free runner instead of a
// ScalaTest suite, because they live in the *main* source file (as requested):
// `sbt test` only compiles src/test, so a framework suite placed here would
// never be picked up. A self-contained `main` keeps the tests in this file AND
// runnable -- via:   sbt "chapter4/runMain DesktopSearchTests"
//
// Coverage (every public behavior of the example):
//   FileCrawler  1. enqueues every file in a flat directory
//                2. recurses into nested sub-directories
//                3. enqueues files only -- never directory entries
//                4. honors the supplied FileFilter
//                5. tolerates a non-existent root (listFiles returns null)
//                6. tolerates an empty directory
//                7. exits cleanly when interrupted while blocked on put()
//   Indexer      8. drains files from the queue
//                9. parks on take() (stays alive) while the queue is empty
//               10. exits cleanly when interrupted
//   Integration 11. crawler -> queue -> indexer end-to-end hand-off
//               12. startIndexing wires producers + consumers and indexes
// ===========================================================================
object DesktopSearchTests {

  private var passed = 0
  private var failed = 0

  /** A FileFilter that accepts everything -- same as the demo's `_ => true`. */
  private val acceptAll: java.io.FileFilter = _ => true

  /** Runs one named test; a thrown AssertionError (or anything) means FAIL. */
  private def test(name: String)(body: => Unit): Unit =
    try {
      body
      passed += 1
      println(s"  [PASS] $name")
    } catch {
      case e: Throwable =>
        failed += 1
        println(s"  [FAIL] $name")
        println(s"         $e")
    }

  /** Minimal assertion -- throws so the enclosing `test` block records a FAIL. */
  private def expect(cond: Boolean, msg: => String): Unit =
    if (!cond) throw new AssertionError(msg)

  /** Polls `cond` until it is true or `timeoutMs` elapses; returns the result. */
  private def waitUntil(timeoutMs: Long)(cond: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!cond && System.currentTimeMillis() < deadline) Thread.sleep(10)
    cond
  }

  // --- temp-filesystem helpers (everything is marked deleteOnExit) ----------

  private def newTempDir(): File = {
    val d = Files.createTempDirectory("desktop-search-test").toFile
    d.deleteOnExit()
    d
  }

  private def newFile(dir: File, name: String): File = {
    val f = new File(dir, name)
    f.createNewFile()
    f.deleteOnExit()
    f
  }

  private def newSubDir(dir: File, name: String): File = {
    val d = new File(dir, name)
    d.mkdir()
    d.deleteOnExit()
    d
  }

  /** Snapshot of the queue's current contents as a Set, for order-free asserts. */
  private def queueContents(q: BlockingQueue[File]): Set[File] =
    q.toArray(new Array[File](0)).toSet

  def main(args: Array[String]): Unit = {
    println("Running DesktopSearch tests...\n")

    // -- 1 -- A flat directory: every file should land on the queue.
    test("FileCrawler enqueues every file in a flat directory") {
      val dir   = newTempDir()
      val files = Set("a.txt", "b.txt", "c.txt").map(newFile(dir, _))
      val q     = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, acceptAll, dir).run() // run() is synchronous, terminates
      expect(queueContents(q) == files, s"expected $files but got ${queueContents(q)}")
    }

    // -- 2 -- crawl() recurses, so files at any depth must be discovered.
    test("FileCrawler recurses into nested sub-directories") {
      val dir      = newTempDir()
      val top      = newFile(dir, "top.txt")
      val sub      = newSubDir(dir, "sub")
      val nested   = newFile(sub, "nested.txt")
      val deep     = newSubDir(sub, "deep")
      val deepFile = newFile(deep, "deep.txt")
      val q        = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, acceptAll, dir).run()
      expect(
        queueContents(q) == Set(top, nested, deepFile),
        s"expected the 3 files at all depths but got ${queueContents(q)}"
      )
    }

    // -- 3 -- Directories are recursed into, never put on the queue.
    test("FileCrawler enqueues files only -- never directory entries") {
      val dir = newTempDir()
      newFile(dir, "file.txt")
      newSubDir(dir, "subdir")
      val q = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, acceptAll, dir).run()
      expect(queueContents(q).forall(_.isFile), s"a directory leaked onto the queue: ${queueContents(q)}")
    }

    // -- 4 -- The FileFilter must exclude non-matching files.
    test("FileCrawler honors the supplied FileFilter") {
      val dir     = newTempDir()
      val txt     = newFile(dir, "keep.txt")
      newFile(dir, "skip.log")
      val onlyTxt: java.io.FileFilter = f => f.getName.endsWith(".txt")
      val q       = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, onlyTxt, dir).run()
      expect(queueContents(q) == Set(txt), s"filter ignored; got ${queueContents(q)}")
    }

    // -- 5 -- listFiles() returns null for a missing root; must not throw.
    test("FileCrawler tolerates a non-existent root") {
      val missing = new File("no-such-path-" + System.nanoTime())
      val q       = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, acceptAll, missing).run()
      expect(q.isEmpty, "a non-existent root should yield an empty queue")
    }

    // -- 6 -- An empty directory yields nothing, without error.
    test("FileCrawler tolerates an empty directory") {
      val dir = newTempDir()
      val q   = new LinkedBlockingQueue[File](100)
      new FileCrawler(q, acceptAll, dir).run()
      expect(q.isEmpty, "an empty directory should yield an empty queue")
    }

    // -- 7 -- A capacity-1 queue makes put() block; interrupt must end run().
    test("FileCrawler exits cleanly when interrupted while blocked on put()") {
      val dir = newTempDir()
      (1 to 10).foreach(i => newFile(dir, s"f$i.txt"))
      val q       = new LinkedBlockingQueue[File](1) // fills after one file
      val crawler = new Thread(new FileCrawler(q, acceptAll, dir))
      crawler.start()
      Thread.sleep(200) // let it fill the slot and block on the next put()
      expect(crawler.isAlive, "crawler should be blocked on a full queue")
      crawler.interrupt()
      crawler.join(1000)
      expect(!crawler.isAlive, "crawler should terminate after interruption")
    }

    // -- 8 -- The consumer pulls items off until the queue is empty.
    test("Indexer drains files from the queue") {
      val dir = newTempDir()
      val q   = new LinkedBlockingQueue[File](100)
      (1 to 5).foreach(i => q.put(newFile(dir, s"f$i.txt")))
      val indexer = new Thread(new Indexer(q))
      indexer.start()
      val drained = waitUntil(2000)(q.isEmpty)
      indexer.interrupt()
      indexer.join(1000)
      expect(drained, "indexer should have drained the queue")
    }

    // -- 9 -- take() parks the thread; an idle indexer must stay alive.
    test("Indexer parks on take() while the queue is empty") {
      val q       = new LinkedBlockingQueue[File](100)
      val indexer = new Thread(new Indexer(q))
      indexer.start()
      Thread.sleep(200)
      val aliveWhileIdle = indexer.isAlive
      indexer.interrupt()
      indexer.join(1000)
      expect(aliveWhileIdle, "an idle indexer should block on take(), not terminate")
    }

    // -- 10 -- Interrupting a blocked indexer must end run().
    test("Indexer exits cleanly when interrupted") {
      val q       = new LinkedBlockingQueue[File](100)
      val indexer = new Thread(new Indexer(q))
      indexer.start()
      Thread.sleep(100)
      indexer.interrupt()
      indexer.join(1000)
      expect(!indexer.isAlive, "indexer should terminate after interruption")
    }

    // -- 11 -- The full pipeline: crawler produces, indexer consumes.
    test("producer and consumer move files end-to-end through the shared queue") {
      val dir   = newTempDir()
      val files = (1 to 8).map(i => newFile(dir, s"f$i.txt")).toSet
      val q     = new LinkedBlockingQueue[File](100)
      val crawler = new Thread(new FileCrawler(q, acceptAll, dir))
      val indexer = new Thread(new Indexer(q))
      indexer.start()
      crawler.start()
      crawler.join(2000)
      val drained = waitUntil(2000)(q.isEmpty)
      indexer.interrupt()
      indexer.join(1000)
      expect(!crawler.isAlive, "crawler should finish after walking the tree")
      expect(files.nonEmpty && drained, "every crawled file should be consumed")
    }

    // -- 12 -- startIndexing wires it all together; capture stdout to confirm
    //          the indexers actually ran. Threads created inside Console.withOut
    //          inherit the redirected stream, so the indexers' prints land here.
    test("startIndexing wires producers + consumers and indexes a directory") {
      val dir = newTempDir()
      (1 to 6).foreach(i => newFile(dir, s"f$i.txt"))
      val buffer = new java.io.ByteArrayOutputStream()
      Console.withOut(new java.io.PrintStream(buffer)) {
        DesktopSearch.startIndexing(Array(dir))
        Thread.sleep(1000) // let crawler + indexers run
      }
      val output = buffer.toString
      expect(output.contains("indexing"), s"startIndexing produced no indexing output: '$output'")
    }

    println(s"\n$passed passed, $failed failed")
    // startIndexing (test 12) leaves non-daemon indexer threads looping
    // forever, so exit explicitly with a status code reflecting the result.
    System.exit(if (failed == 0) 0 else 1)
  }
}