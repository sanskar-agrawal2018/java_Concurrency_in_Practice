/**
 * Scala class and constructor demo.
 *
 * In Scala, the class body is part of the primary constructor. That means:
 *   - field initializers run when you create the object
 *   - plain statements in the class body run when you create the object
 *   - def methods run only when you call them
 *
 * This file shows:
 *   1) what executes during construction
 *   2) how an auxiliary constructor works
 *   3) how superclass construction happens before subclass body code
 *   4) constructor principles suitable for production code
 */

// The println calls in this class are intentional: they demonstrate execution
// order. Production constructors should normally avoid I/O and global effects.
class Person(name: String, age: Int) {
  println(s"[Person ctor] start for name=$name, age=$age")

  private val normalizedName: String = {
    println("[Person field] normalizing name during construction")
    name.trim
  }

  private val validatedAge: Int = {
    println("[Person field] validating age during construction")
    require(age >= 0, "age must be non-negative")
    age
  }

  println("[Person ctor] body continues after field initialization")

  def this(name: String) = {
    this(name, 0)
    println(s"[Person aux ctor] default age applied for $normalizedName")
  }

  def sayHello(): Unit = {
    println(s"[Person method] Hello, my name is $normalizedName and I am $validatedAge years old.")
  }

  def compare(other: Person): Boolean = {
    println("[Person method] compare() is running now")
    this.normalizedName == other.normalizedName
  }
}

class Specific(name: String, age: Int, role: String) extends Person(name, age) {
  println(s"[Specific ctor] role=$role")

  private val roleLabel: String = {
    println("[Specific field] preparing role label during construction")
    role.trim
  }

  def work(): Unit = {
    println(s"[Specific method] working as $roleLabel")
  }
  println(s"[Specific ctor] body continues after superclass constructor")
}

/**
 * Production-style constructor principles:
 *   - keep initialized state immutable
 *   - validate and normalize input before creating the object
 *   - use a private constructor and factory when creation can fail
 *   - mark the class final unless inheritance is intentionally supported
 *   - keep constructors free from I/O, threads, shutdown hooks, and global state
 */
final class WellDesignedPerson private (
    val name: String,
    val age: Int
) {
  def greeting: String =
    s"Hello, my name is $name and I am $age years old."
}

object WellDesignedPerson {
  def create(name: String, age: Int): Option[WellDesignedPerson] = {
    val normalizedName = name.trim

    if (normalizedName.isEmpty || age < 0)
      None
    else
      Some(new WellDesignedPerson(normalizedName, age))
  }
}

object Class {
  def main(args: Array[String]): Unit = {
    println("=== 1) Class body vs constructor code ===")
    println("[main] Before new Person(\"Alice\", 30)")
    val alice = new Person("Alice", 30)
    println("[main] After new Person(\"Alice\", 30)")
    alice.sayHello()

    println()
    println("=== 2) Auxiliary constructor ===")
    println("[main] Before new Person(\"Bob\")")
    val bob = new Person("Bob")
    println("[main] After new Person(\"Bob\")")
    bob.sayHello()

    println()
    println("=== 3) Superclass constructor before subclass body ===")
    println("[main] Before new Specific(\"Carol\", 28, \"Engineer\")")
    val carol = new Specific("Carol", 28, "Engineer")
    println("[main] After new Specific(\"Carol\", 28, \"Engineer\")")
    carol.sayHello()
    carol.work()

    println()
    println("=== 4) Production-style constructor design ===")
    val validPerson = WellDesignedPerson.create("  David  ", 35)
    val invalidPerson = WellDesignedPerson.create("", -1)

    validPerson match {
      case Some(person) => println(s"[main] Valid object: ${person.greeting}")
      case None         => println("[main] Person could not be created")
    }

    invalidPerson match {
      case Some(person) => println(s"[main] Valid object: ${person.greeting}")
      case None         => println("[main] Invalid object rejected")
    }

    println()
    println("=== Good coding principles ===")
    println("- Code in the class body and field initializers runs during object creation.")
    println("- Code in a def method runs only when you call the method.")
    println("- An auxiliary constructor adds extra setup after the primary constructor runs.")
    println("- In inheritance, the superclass constructor runs before the subclass body.")
    println("- Constructors should establish a valid object using small, deterministic operations.")
    println("- Prefer immutable val fields and explicit constructor dependencies.")
    println("- Use Option from a factory when only success or failure matters.")
    println("- Avoid I/O, threads, shutdown hooks, and global-state changes in constructors.")
  }
}
