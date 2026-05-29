
class Person(name: String, age: Int) {
  private val name1:String = name
  private[this] val age1:Int = age
  protected val name3:String =name3
  val name2:String = name
  def sayHello(): Unit = {
    println(s"Hello, my name is $name1 and I am $age1 years old.")
    println(s"Accessing name1: $age1")
  }


  def compare(p:Person):Boolean ={
      this.name1 == p.name1
  }
}


class Specific(name: String, age: Int) extends Person(name: String, age: Int) {
  def nameReturn:String = name3
}

object Class {
  def main(args: Array[String]): Unit = {
    val person: Person = new Specific("Alice", 30)
    person.sayHello()
    println(s"Accessing name1 from outside: ${person.name2}")

    println(s"Accessing name1 from outside: ${person}")
  }

}

