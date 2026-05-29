/*
 * Blocking Queues  (java.util.concurrent.BlockingQueue and friends)
 *
 * A BLOCKING QUEUE is a thread-safe FIFO with built-in flow control: a
 * producer that tries to put() into a full queue blocks until space frees up,
 * and a consumer that tries to take() from an empty queue blocks until an
 * element arrives. This makes blocking queues the standard plumbing for the
 * PRODUCER-CONSUMER pattern -- almost every other concurrent primitive in
 * java.util.concurrent (executors, thread pools, CompletionService) is built
 * on top of one.
 *
 * The interface has FOUR families of operations, varying by what they do on
 * failure (queue full on insert, queue empty on remove):
 *
 *   action      throws         returns      blocks            blocks up to t
 *   --------    -----------    ---------    --------------    ----------------
 *   insert      add(e)         offer(e)     put(e)            offer(e,t,unit)
 *   remove      remove()       poll()       take()            poll(t,unit)
 *   peek-only   element()      peek()       --                --
 *
 * Pick the row by the contract you want; pick the column by what should
 * happen when the queue can't satisfy you right now. For producer/consumer
 * pipelines, put/take are almost always correct.
 *
 * Common implementations:
 *
 *   ArrayBlockingQueue(N)           bounded, array-backed, single lock.
 *                                   Predictable memory; back-pressure built in.
 *   LinkedBlockingQueue([N])        optionally bounded, linked-node, TWO locks
 *                                   (head + tail) -- higher throughput under
 *                                   contention. UNBOUNDED by default -- a
 *                                   slow consumer can OOM you. Always set a
 *                                   capacity in production.
 *   SynchronousQueue                "queue" of capacity zero. Each put() hands
 *                                   directly off to a take() and vice versa --
 *                                   used by Executors.newCachedThreadPool.
 *   PriorityBlockingQueue           unbounded heap-ordered queue.
 *   DelayQueue                      heads pop only when their delay elapses.
 *
 * Why blocking queues are the producer-consumer answer:
 *   - they bound memory (when capacity is finite) so a fast producer can't
 *     drown a slow consumer;
 *   - they decouple producers and consumers in time and in count;
 *   - they handle all the wait/notify boilerplate (and its pitfalls) for you.
 *
 * Shutdown / termination: a blocking queue has NO built-in "no more items
 * coming" signal. Two common patterns: (1) interrupt the consumer threads --
 * take() throws InterruptedException; (2) send a sentinel "poison pill"
 * element that consumers recognise and exit on. Demo D shows the latter.
 */

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

object BlockingQueues {

  // -------------------------------------------------------------------------
  // Demo A -- producer/consumer: 1 producer, 2 consumers, bounded queue.
  // The bound applies back-pressure on the producer when consumers fall behind.
  // -------------------------------------------------------------------------
  private def demoProducerConsumer(): Unit = {
    println("[A] one producer, two consumers, ArrayBlockingQueue(4) -- bounded back-pressure")
    val queue: BlockingQueue[Int] = new ArrayBlockingQueue[Int](4)
    val produced = 12
    val taken    = new AtomicInteger(0)
    val pool     = Executors.newFixedThreadPool(3)

    pool.submit(new Runnable {
      def run(): Unit = {
        for (i <- 1 to produced) {
          queue.put(i)                          // blocks if the queue is full
          println(s"[A]   producer put $i; size=${queue.size}")
        }
      }
    })

    val consumer = new Runnable {
      def run(): Unit = {
        try {
          while (taken.get() < produced) {
            val v = queue.take()                // blocks if the queue is empty
            taken.incrementAndGet()
            println(s"[A]   ${Thread.currentThread().getName} took $v")
            Thread.sleep(40)                    // simulate slow consumption
          }
        } catch { case _: InterruptedException => Thread.currentThread().interrupt() }
      }
    }
    pool.submit(consumer); pool.submit(consumer)

    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.SECONDS)
    println(s"[A]   done: taken=${taken.get}, remaining in queue=${queue.size}")
  }

  // -------------------------------------------------------------------------
  // Demo B -- non-blocking variants: offer() and poll() return immediately
  // with a Boolean / null instead of waiting. Use them when "give up now" is
  // an acceptable answer.
  // -------------------------------------------------------------------------
  private def demoOfferPoll(): Unit = {
    println("[B] offer / poll: never block; report success/failure instead")
    val q: BlockingQueue[String] = new ArrayBlockingQueue[String](2)
    println(s"[B]   offer A -> ${q.offer("A")}")
    println(s"[B]   offer B -> ${q.offer("B")}")
    println(s"[B]   offer C -> ${q.offer("C")}   <-- queue full, returns false")

    println(s"[B]   poll  -> ${q.poll()}")
    println(s"[B]   poll  -> ${q.poll()}")
    println(s"[B]   poll  -> ${q.poll()}             <-- queue empty, returns null")
  }

  // -------------------------------------------------------------------------
  // Demo C -- timed variants: offer/poll(t, unit) wait up to t and then give
  // up. Useful for "try hard, then degrade" logic in clients.
  // -------------------------------------------------------------------------
  private def demoTimed(): Unit = {
    println("[C] timed offer/poll: wait up to a deadline, then return false/null")
    val q: BlockingQueue[Int] = new ArrayBlockingQueue[Int](1)
    q.put(99)                                   // fill it so the next offer must wait

    val started = System.currentTimeMillis()
    val ok = q.offer(100, 80, TimeUnit.MILLISECONDS)
    val elapsed = System.currentTimeMillis() - started
    println(s"[C]   offer(100, 80ms) -> $ok after ~${elapsed}ms")

    println(s"[C]   poll(50ms) on a queue with one element -> ${q.poll(50, TimeUnit.MILLISECONDS)}")
    println(s"[C]   poll(50ms) on the now-empty queue       -> ${q.poll(50, TimeUnit.MILLISECONDS)}")
  }

  // -------------------------------------------------------------------------
  // Demo D -- POISON PILL shutdown: the producer sends a sentinel value that
  // every consumer recognises and exits on. This is the canonical way to
  // gracefully drain a producer/consumer pipeline.
  // -------------------------------------------------------------------------
  private def demoPoisonPill(): Unit = {
    println("[D] poison pill: producer signals shutdown with a sentinel element")
    val PILL = -1
    val q: BlockingQueue[Int] = new ArrayBlockingQueue[Int](8)
    val nConsumers = 3
    val pool = Executors.newFixedThreadPool(nConsumers + 1)

    pool.submit(new Runnable {
      def run(): Unit = {
        for (i <- 1 to 7) q.put(i)              // real work items
        for (_ <- 1 to nConsumers) q.put(PILL)  // one pill per consumer
        println("[D]   producer finished and sent the pills")
      }
    })

    for (c <- 1 to nConsumers) pool.submit(new Runnable {
      def run(): Unit = {
        try {
          var stop = false
          while (!stop) {
            val v = q.take()
            if (v == PILL) {
              println(s"[D]   consumer-$c saw the pill; exiting")
              stop = true
            } else {
              println(s"[D]   consumer-$c processed $v")
            }
          }
        } catch { case _: InterruptedException => Thread.currentThread().interrupt() }
      }
    })

    pool.shutdown()
    pool.awaitTermination(5, TimeUnit.SECONDS)
  }

  def main(args: Array[String]): Unit = {
    demoProducerConsumer(); println()
    demoOfferPoll();        println()
    demoTimed();            println()
    demoPoisonPill()
  }
}