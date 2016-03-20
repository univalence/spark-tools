package net.ironforged.scaladiff

object OperationType extends CustomEnum {
  case object Insert extends OperationType.Value; Insert
  case object Delete extends OperationType.Value; Delete
  case object Equals extends OperationType.Value; Equals
}

case class Operation(op: OperationType.Value, text: String) {
  override def toString(): String = {
    import OperationType._
    op match {
      case Insert => s"+$text"
      case Delete => s"-$text"
      case Equals => text
    }
  }
}
