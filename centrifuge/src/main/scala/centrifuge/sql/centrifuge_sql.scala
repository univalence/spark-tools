package org.apache.spark.sql

import io.univalence.centrifuge.AnnotationSql
import io.univalence.centrifuge.Result
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.catalyst.expressions.CreateStruct
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object centrifuge_sql {

  class QATools(val sparkSession: SparkSession) {

    def registerTransformation[A: TypeTag, B: TypeTag](name: String, f: A => Result[B]): UserDefinedFunction = {

      val udf: UserDefinedFunction = sparkSession.udf.register("qa_raw_" + name, f)

      val expressionsToField: Seq[Expression] => GetStructField = (s: Seq[Expression]) =>
        GetStructField(
          child   = udf.apply(s.map(Column.apply): _*).expr,
          ordinal = 0,
          name    = Some("value")
      )

      /*
      sparkSession.sessionState.functionRegistry.registerFunction(
        name,
        expressionsToField
      )*/

      val registry = sparkSession.sessionState.functionRegistry

      //TODO update the call for spark 2.4
      val method = registry.getClass.getMethod("registerFunction", classOf[String], classOf[FunctionBuilder])
      method.invoke(registry, name, expressionsToField)

      udf
    }

  }

  private def cleanAnnotation(s: Seq[Any]): Seq[AnnotationSql] = {

    def cleanRow(a: Any): Seq[AnnotationSql] = {

      def cleanInerRow(aa: Any, onField: String, fromFields: Seq[String]): Seq[AnnotationSql] =
        aa match {
          case m: Map[String, Any] =>
            m.toSeq.flatMap(x => cleanInerRow(x._2, onField, fromFields))
          case row: GenericRowWithSchema =>
            Seq(
              AnnotationSql(
                msg        = row.getAs[String](0),
                onField    = onField,
                fromFields = fromFields.toVector,
                isError    = row.getAs[Boolean](3),
                count      = row.getAs[Long](4)
              )
            )

          case wa: mutable.WrappedArray[Any] =>
            wa.flatMap(x => cleanInerRow(x, onField, fromFields))
          case _ => println(aa.getClass + " : " + aa); Nil
        }

      a match {
        case e: GenericRowWithSchema =>
          cleanInerRow(e.get(2), e.getAs[String](1), e.getAs[Seq[String]](3))
      }
    }

    annotationsFusion(s.flatMap(cleanRow))
  }

  private def annotationsFusion[T](s: Seq[T]): Seq[T] =
    if (s.isEmpty) {
      s
    } else
      s.head match {
        case _: AnnotationSql =>
          s.asInstanceOf[Seq[AnnotationSql]]
            .groupBy(x => (x.fromFields, x.isError, x.message, x.onField))
            .values
            .map(x => {
              x.head.copy(count = x.map(_.count).sum)
            })
            .toSeq
            .asInstanceOf[Seq[T]]
        case _: GenericRowWithSchema =>
          s.asInstanceOf[Seq[GenericRowWithSchema]]
            .groupBy(x => x.toSeq.updated(x.fieldIndex("count"), 0))
            .values
            .map(x => {
              new GenericRowWithSchema(
                x.head.toSeq
                  .updated(x.head.fieldIndex("count"), x.map(_.getAs[Long]("count")).sum)
                  .toArray,
                x.head.schema
              )
            })
            .toSeq
            .asInstanceOf[Seq[T]]
        case _ => ???
      }

  private def mergeAnnotations(s: Seq[Any]): Seq[AnnotationSql] =
    annotationsFusion(s.asInstanceOf[Seq[Seq[Any]]].flatten.asInstanceOf[Seq[AnnotationSql]])

  case class QAUdfInPlan(tocol: String, udf: ScalaUDF, fromFields: Seq[String])

  class QADF(val dataFrame: DataFrame) {

    private def findColChildDeep(exp: Expression): Seq[String] =
      exp match {
        case AttributeReference(name, _, _, _) => Seq(name)
        case _ =>
          exp.children.toList match {
            case Nil =>
              println(exp.getClass + " : " + exp); Nil
            case xs: List[Expression] => xs.flatMap(findColChildDeep)
          }
      }

    private def findColChild(scalaUdf: ScalaUDF): Seq[String] =
      scalaUdf.children.flatMap(findColChildDeep)

    private def recursivelyFindScalaUDF(exp: Expression, tocol: String): Seq[QAUdfInPlan] =
      exp match {
        case s: ScalaUDF => Seq(QAUdfInPlan(tocol, s, findColChild(s)))
        case _           => exp.children.flatMap(x => recursivelyFindScalaUDF(x, tocol))
      }

    private def recursivelyFindScalaUDF(expressions: Seq[Expression]): Seq[QAUdfInPlan] =
      expressions.flatMap({
        case Alias(child, name) => recursivelyFindScalaUDF(child, name)
        case x                  => println(x); Nil

      })

    def includeSources: DataFrame = {

      val sparkSession = dataFrame.sparkSession

      val plan: LogicalPlan = dataFrame.queryExecution.optimizedPlan

      println(plan.toJSON)

      ???
    }

    def includeRejectFlags: DataFrame =
      //ajout des champs pour avec un boolean qui dit s'il y a un champ en erreur, et un autre pour dire s'il y a des warnings
      ???

    /*

actual parameters "java.lang.String, scala.Option, scala.collection.Seq, boolean, long"; c
andidates are: "io.univalence.centrifuge.Annotation(java.lang.String, scala.Option, scala.collection.immutable.Vector, boolean, long)"

     */

    //TODO : test to check schema ...
    private val annotationsDt =
      ArrayType(
        StructType(
          Seq(
            StructField(name = "message", dataType    = StringType, nullable = false),
            StructField(name = "onField", dataType    = StringType, nullable = true),
            StructField(name = "fromFields", dataType = ArrayType(StringType, containsNull = false), nullable = false),
            StructField(name = "isError", dataType    = BooleanType, nullable = false),
            StructField(name = "count", dataType      = LongType, nullable = false)
          )
        ),
        containsNull = false
      )

    private val emptyAnnotation =
      Literal(ArrayData.toArrayData(Nil), annotationsDt)

    private def recursiveNewPlan(
      logicalPlan: LogicalPlan,
      sparkSession: SparkSession
    ): (LogicalPlan, Seq[Attribute]) = {

      val res = logicalPlan match {
        case Project(projectList, child) =>
          val (newChild, attributes) = recursiveNewPlan(child, sparkSession)

          val anns: Expression =
            createCurrentLevel(logicalPlan.expressions, sparkSession)

          val merged: Expression = if (attributes.nonEmpty) {
            val mergeAnnotationUDF = sparkSession.udf
              .register("qa_internal_merge_annotations", mergeAnnotations _)

            mergeAnnotationUDF(Column(CreateArray(anns +: attributes))).expr
          } else anns

          val anncol: NamedExpression = Alias(merged, "annotations")()

          (Project(projectList :+ anncol, newChild), Seq(anncol.toAttribute))

        case Join(left, right, joinType, condition) =>
          val plan1 = recursiveNewPlan(left, sparkSession)
          val plan2 = recursiveNewPlan(right, sparkSession)
          (
            Join(
              left      = plan1._1,
              right     = plan2._1,
              joinType  = joinType,
              condition = condition
            ),
            plan1._2 ++ plan2._2
          )

        case agg @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
          val mergeAnnotationUDF = sparkSession.udf
            .register("qa_internal_merge_annotations", mergeAnnotations _)

          val (newChild, attributes) = recursiveNewPlan(child, sparkSession)

          val currentLevel: Expression =
            createCurrentLevel(aggregateExpressions, sparkSession)

          val annotationCol: NamedExpression =
            Alias(mergeAnnotationUDF(Column(CreateArray(attributes :+ currentLevel))).expr, "annotations")()

          val collected = Alias(
            mergeAnnotationUDF(
              Column(
                CollectList(annotationCol.toAttribute)
                  .toAggregateExpression(false)
              )
            ).expr,
            "annotations"
          )()

          (
            Aggregate(
              groupingExpressions,
              aggregateExpressions :+ collected,
              Project(Seq(UnresolvedStar(None), annotationCol), newChild)
            ),
            Seq(collected.toAttribute)
          )

        case x: ObjectConsumer =>
          (Project(Seq(UnresolvedStar(None), Alias(emptyAnnotation, "annotations")()), x), Nil)
        case x =>
          (Project(Seq(UnresolvedStar(None), Alias(emptyAnnotation, "annotations")()), x), Nil)

      }
      res

    }

    private def createCurrentLevel(expressions: Seq[Expression], sparkSession: SparkSession) = {
      val collect: Seq[QAUdfInPlan] = recursivelyFindScalaUDF(expressions)
      val anns: Expression = if (collect.isEmpty) {
        emptyAnnotation
      } else {
        val cleanAnnotationUDF = sparkSession.udf
          .register("qa_internal_clean_annotations", cleanAnnotation _)

        val array = CreateArray(
          collect.map(
            d =>
              CreateStruct(
                Seq(
                  Literal("udfinfo"),
                  Literal(d.tocol),
                  GetStructField(d.udf, 1, Some("annotations")),
                  CreateArray(d.fromFields.distinct.sorted.map(x => Literal(x)))
                )
            )
          )
        )

        val cleaned: Column = cleanAnnotationUDF(Column(array))
        cleaned.expr
      }
      anns
    }

    def includeAnnotations: DataFrame = {

      val sparkSession = dataFrame.sparkSession

      val plan: LogicalPlan = dataFrame.queryExecution.optimizedPlan

      val newPlan: LogicalPlan = recursiveNewPlan(plan, sparkSession)._1

      val ndf = new Dataset[Row](
        sqlContext = sparkSession.sqlContext,
        newPlan,
        RowEncoder(sparkSession.sessionState.executePlan(newPlan).analyzed.schema)
      )

      ndf
    }
  }

}
