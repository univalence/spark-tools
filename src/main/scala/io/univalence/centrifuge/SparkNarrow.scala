package io.univalence.centrifuge

import org.apache.spark.sql.types.{DataType, StructType}

sealed trait SType {
  def typeName: String
}

case class SOption(arg: SType) extends SType {
  override def typeName: String = s"Option[${arg.typeName}]"
}
case class SClass(name: String) extends SType {
  override def typeName: String = name
}

case class SCC(names: Seq[String], args: Seq[(String, SType)]) extends SType {

  def classDef: String =
    s"case class ${names.last}(${args.map({ case (n, t) ⇒ s"$n:${t.typeName}" }).mkString(",")} )"

  override def typeName: String = names.mkString(".")
}

object Sparknarrow {

  def dataTypeToTypeName(dataType: DataType): String = {
    dataType.simpleString.capitalize match {
      case "Date" ⇒ "java.sql.Date"
      case "Int"  ⇒ "scala.Int"
      case x      ⇒ s"java.lang.$x"
    }
  }

  def basicCC(schema: StructType, pck: Option[String] = None, name: String = "_Cc"): SCC = {
    SCC(
      names = pck.toSeq ++ List(name),
      schema.map(strucField ⇒ {
        strucField.name → SOption(SClass(dataTypeToTypeName(strucField.dataType)))
      })
    )
  }

}
