package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{LongType, TimestampType}


object MyUDF {


  private def myTimestampCast(xs:Seq[Expression]):Expression = {
    val expSource = xs.head
    expSource.dataType match {
      case LongType => new Column(expSource).divide(Literal(1000)).cast (TimestampType).expr
      case TimestampType => /* WARNING */ expSource
    }
  }


  def register(sparkSession:SparkSession):Unit = {
    sparkSession
      .sessionState
      .functionRegistry
      .registerFunction("toTs",myTimestampCast)
  }


}