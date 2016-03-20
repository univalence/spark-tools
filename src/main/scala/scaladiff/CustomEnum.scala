package net.ironforged.scaladiff

trait CustomEnum {

  type U <: Value

  abstract class Value (named: String = "") {

    values.lastOption.map {
      case (id, _) => {
        _values += (id + 1) -> this.asInstanceOf[U]
      }
    }.getOrElse {
      _values += 0 -> this.asInstanceOf[U]
    }

    val name = if (named.isEmpty) this.toString else named
    val id   = _values.find(v => v._2 == this).get._1
  }

  private var _values = Map.empty[Int, U]
  def values = _values.toSeq.sortBy(_._1)

  def apply(x: Int): Option[U] = {
    _values.find(v => v._1 == x).map(_._2)
  }

  def apply(x: String): Option[U] = {
    _values.find(v => v._2.name == x).map(_._2)
  }
}
