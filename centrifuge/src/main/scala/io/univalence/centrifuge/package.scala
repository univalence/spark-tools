package io.univalence

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.centrifuge_sql._

package object centrifuge {

  type AnnotationSql = Annotation

  object AnnotationSql {

    def apply(
        msg:        String,
        onField:    String,
        fromFields: Vector[String],
        isError:    Boolean,
        count:      Long
    ): Annotation = Annotation(
      message    = msg,
      isError    = isError,
      count      = count,
      onField    = Some(onField),
      fromFields = fromFields
    )

  }

  object implicits {
    implicit def QADFOps[T](dataframe: Dataset[T]): QADF =
      new QADF(dataframe.toDF())
    implicit def sparkSessionOps(ss: SparkSession): QATools = new QATools(ss)
  }

}
