package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.TimestampType

object MyUDF {

  private def myTimestampCast(xs: Seq[Expression]): Expression = {
    val expSource = xs.head
    expSource.dataType match {
      case LongType =>
        new Column(expSource).divide(Literal(1000)).cast(TimestampType).expr
      case TimestampType =>
        /* WARNING */
        expSource
    }
  }

  def register(sparkSession: SparkSession): Unit =
    sparkSession.sessionState.functionRegistry
      .registerFunction("toTs", myTimestampCast)

}
