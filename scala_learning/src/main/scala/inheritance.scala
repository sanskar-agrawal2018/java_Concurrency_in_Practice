
class person {
  var name: String = _
  var age: Int = _

  def this(name: String, age: Int) {
    this()
    this.name = name
    this.age = age
  }

  def show(): Unit = {
    println(s"name: $name, age: $age")
  }
}



class student extends person {
  var grade: String = _

  def this(name: String, age: Int, grade: String) {
    this()
    this.name = name
    this.age = age
    this.grade = grade
  }

  override def show(): Unit = {
    super.show()
    println(s"grade: $grade")
  }


  def test():Unit ={
    println(s"name: $name, age: $age, grade: $grade")
  }


}


object  inheritance extends App {
  val s:student = new student("Alice", 20, "A")
  s.show()
  s.test()

}
