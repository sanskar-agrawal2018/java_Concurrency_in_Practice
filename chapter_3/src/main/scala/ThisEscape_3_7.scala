/*
 * Listing 3.7 - ThisEscape
 *
 * "A final mechanism by which an object or its internal state can be published
 *  is to publish an inner class instance. When ThisEscape publishes the
 *  EventListener, it implicitly publishes the enclosing ThisEscape instance as
 *  well, because inner class instances contain a hidden reference to the
 *  enclosing instance."  -- Java Concurrency in Practice, 3.2
 *
 * The bug: the listener is handed to the EventSource WHILE the ThisEscape
 * constructor is still running. Through the listener's hidden reference to the
 * enclosing object, another thread can call into a ThisEscape that is not yet
 * fully built -- it can see fields in their default (null / 0) state.
 */

// ---------------------------------------------------------------------------
// Supporting types: an event, a listener interface, and a source that other
// threads can use to register listeners and fire events.
// ---------------------------------------------------------------------------

final case class Event(message: String)

trait EventListener {
  def onEvent(e: Event): Unit
}

class EventSource {
  // Guarded by `this`. Other threads register listeners and fire events here.
  private val listeners = scala.collection.mutable.ListBuffer.empty[EventListener]

  def registerListener(l: EventListener): Unit = synchronized {
    listeners += l
  }

  def fireEvent(e: Event): Unit = {
    val snapshot = synchronized(listeners.toList)
    snapshot.foreach(_.onEvent(e))
  }
}

// ---------------------------------------------------------------------------
// UNSAFE: `this` escapes during construction.
//
// The `new EventListener { ... }` below is an anonymous inner class. In Scala
// (and Java) such an instance carries a hidden reference to the *enclosing*
// ThisEscape object so that `onEvent` can call `doSomething`. The moment we
// pass that listener to `source.registerListener`, the half-built ThisEscape
// becomes reachable from any thread holding the EventSource.
// ---------------------------------------------------------------------------

class ThisEscape(source: EventSource) {

  // Step 1: register the inner listener. This publishes `this` IMMEDIATELY,
  // before the fields below are assigned.
  source.registerListener(new EventListener {
    def onEvent(e: Event): Unit = {
      // `doSomething` is a method of the enclosing ThisEscape, reached via the
      // inner class's hidden reference to that enclosing instance.
      doSomething(e)
    }
  })

  // Step 2: simulate the rest of the constructor's work still in progress.
  Thread.sleep(100)

  // Step 3: this field is initialized LAST. If another thread fires an event
  // between Step 1 and Step 3, `name` is still its default value: null.
  private val name: String = "fully-constructed ThisEscape"

  def doSomething(e: Event): Unit =
    println(s"[ThisEscape]   handling '${e.message}'  ->  name = $name")
}

// ---------------------------------------------------------------------------
// SAFE: the fix (Listing 3.8 - SafeListener).
//
// Do not let `this` escape the constructor. Build the object completely with a
// private constructor, and only THEN publish the listener through a factory.
// ---------------------------------------------------------------------------

class SafeListener private () {
  // Fully initialized before anyone outside can see this object.
  private val name: String = "fully-constructed SafeListener"

  // The inner listener still holds a hidden reference to `this`, but `this` is
  // already complete by the time `newInstance` registers it.
  private[this] val listener: EventListener = new EventListener {
    def onEvent(e: Event): Unit = doSomething(e)
  }

  private def doSomething(e: Event): Unit =
    println(s"[SafeListener] handling '${e.message}'  ->  name = $name")

  // Exposed only to the factory, after construction has finished.
  private def register(source: EventSource): Unit =
    source.registerListener(listener)
}

object SafeListener {
  def newInstance(source: EventSource): SafeListener = {
    val safe = new SafeListener() // 1. construct fully
    safe.register(source)         // 2. publish only now
    safe
  }
}

// ---------------------------------------------------------------------------
// Demo: a background thread fires an event while the object is being built.
// ---------------------------------------------------------------------------

object ThisEscapeRun {

  /** Starts a thread that fires an event 30ms from now (mid-construction). */
  private def fireAfter(source: EventSource, label: String): Thread = {
    val t = new Thread(() => {
      Thread.sleep(30)
      source.fireEvent(Event(label))
    })
    t.start()
    t
  }

  def main(args: Array[String]): Unit = {
    println("=== UNSAFE: ThisEscape (this escapes during construction) ===")
    val unsafeSource = new EventSource
    val firer1 = fireAfter(unsafeSource, "early event")
    // Constructor registers the listener, then sleeps 100ms before setting
    // `name`. The firer thread fires at ~30ms, so `name` is still null.
    new ThisEscape(unsafeSource)
    firer1.join()
    println("  -> 'name = null' proves a half-built object was observed.\n")

    println("=== SAFE: SafeListener (published only after construction) ===")
    val safeSource = new EventSource
    val firer2 = fireAfter(safeSource, "early event")
    // newInstance finishes construction BEFORE registering the listener,
    // so any event the firer thread delivers sees a complete object.
    SafeListener.newInstance(safeSource)
    firer2.join()
    println("  -> 'name' is always fully set; no escape occurred.")
  }
}
