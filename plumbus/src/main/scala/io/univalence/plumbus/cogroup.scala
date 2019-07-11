package io.univalence.plumbus

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, KeyValueGroupedDataset, Row, types}

import scala.reflect.ClassTag
import scala.util.Try

object cogroup {

  /**
    * Usage :
    * {{{
    * import io.univalence.plumbus.cogroup._
    *
    * val persons:Dataset[Person] = ???
    * val addresses:Dataset[Address] = ???
    *
    * persons.groupByKey(_.id).cogroup(addresses.groupByKey(_.personId))
    * }}}
    *
    * @param kvgd
    * @tparam K
    * @tparam A
    */
  implicit class KVGD[K, A](val kvgd: KeyValueGroupedDataset[K, A]) {
    def cogroup[B](right: KeyValueGroupedDataset[K, B]): Dataset[(K, Seq[A], Seq[B])] =
      //Use SparkAddOn ?
      ???
  }

  def apply[A, B, K](left: Dataset[A], right: Dataset[B])(keyLeft: A => K, keyRight: B => K)(
    implicit encA: Encoder[A],
    encB: Encoder[B],
    encC: Encoder[K],
    enc: Encoder[(K, Seq[A], Seq[B])],
    ca: ClassTag[A],
    ck: ClassTag[K],
    cb: ClassTag[B]
  ): Dataset[(K, Seq[A], Seq[B])] =
    left.sparkSession.implicits
      .rddToDatasetHolder(
        RDD
          .rddToPairRDDFunctions(left.rdd.keyBy(keyLeft))
          .cogroup(right.rdd.keyBy(keyRight))
          .map({ case (k, (ia, ib)) => (k, ia.toSeq, ib.toSeq) })
      )
      .toDS

  def cogroupDf(group: DataFrame,
                namedSubGroup: (String, DataFrame)*)
               (byKey: String,
                partitioner: Partitioner = Partitioner.defaultPartitioner(group.rdd, namedSubGroup.map(_._2.rdd): _*)
               ): Try[DataFrame] =
    Try {
      val subGroup: Seq[DataFrame] = namedSubGroup.map(_._2)
      val allFrames: Seq[DataFrame] = group +: subGroup
      val allFramesKeyed: Seq[RDD[(String, Row)]] =
        allFrames.map(df => {
          val idx = df.columns.indexOf(byKey)
          df.rdd.keyBy(_.get(idx).toString)
        })

      val cogroupRdd: CoGroupedRDD[String] = new CoGroupedRDD[String](allFramesKeyed, partitioner)

      val rowRdd: RDD[Row] =
        cogroupRdd.map(x => {
          val rows: Array[Seq[Row]] = x._2.asInstanceOf[Array[Iterable[Row]]].map(_.toSeq)
          val seq = rows.head.head.toSeq ++ rows.tail

          new GenericRowWithSchema(seq.toArray, null).asInstanceOf[Row]
        })

      val schema =
        types.StructType(
          group.schema.fields
            ++ namedSubGroup.map { case (name, df) => StructField(name, ArrayType(df.schema)) }
        )

      group.sparkSession.createDataFrame(rowRdd, schema)
    }

}
