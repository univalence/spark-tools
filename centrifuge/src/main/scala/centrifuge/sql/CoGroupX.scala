package centrifuge.sql

import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import shapeless._
import shapeless.ops.hlist.Tupler
import shapeless.ops.traversable.FromTraversable

import scala.language.higherKinds
import scala.reflect.ClassTag

trait CheckCogroup[K, H <: HList] extends Serializable {
  type Res

  def toSeq(h: H): List[RDD[(K, _)]]
}

object CheckCogroup {
  type Aux[K, H <: HList, R] = CheckCogroup[K, H] { type Res = R }

  implicit def rFromNil[K]: CheckCogroup.Aux[K, HNil, HNil] =
    new CheckCogroup[K, HNil] {
      type Res = HNil

      override def toSeq(h: HNil): List[RDD[(K, _)]] = Nil
    }

  implicit def rCons[K, V, REST <: HList, RES <: HList](
      implicit removeFirstT: CheckCogroup.Aux[K, REST, RES]): CheckCogroup.Aux[K, RDD[(K, V)] :: REST, Seq[V] :: RES] =
    new CheckCogroup[K, RDD[(K, V)] :: REST] {
      type Res = Seq[V] :: RES

      //override def toSeq(h: ::[RDD[(K, V)], REST]): List[RDD[(K, _)]] = h.head.map(identity) :: removeFirstT.toSeq(h.tail)
      override def toSeq(h: ::[RDD[(K, V)], REST]): List[RDD[(K, _)]] =
        h.head.asInstanceOf[RDD[(K, _)]] :: removeFirstT.toSeq(h.tail)
    }
}

trait CoGroupN[K, H] extends Serializable {
  type Res <: (K, _)

  def arrayToRes(k: K, array: Array[Iterable[_]]): Res

  def toSeq(h: H): List[RDD[(K, _)]]
}

object CoGroupN {
  type Aux[K, H, R] = CoGroupN[K, H] { type Res = R }

  //FOR THE LULZ, comme si quelqu'un allait faire un group by comme ça !
  //On peut supprimer ce cas, dans le cas d'un group by, le cogroupN reverra un RDD[(K,(Seq[V], Unit))]
  implicit def groupByCase[K, V]: CoGroupN.Aux[K, RDD[(K, V)] :: HNil, (K, Seq[V])] =
    new CoGroupN[K, RDD[(K, V)] :: HNil] {
      type Res = (K, Seq[V])

      override def arrayToRes(k: K, array: Array[Iterable[_]]): (K, Seq[V]) =
        (k, array.head.toSeq.asInstanceOf[Seq[V]])

      override def toSeq(h: ::[RDD[(K, V)], HNil]): List[RDD[(K, _)]] =
        List(h.head.asInstanceOf[RDD[(K, _)]])
    }

  implicit def cogroupNByCase[K, H1 <: RDD[_], HRest <: HList, ABC <: HList, R <: Product](
      implicit
      // verifie que c'est une séquence de RDD[(K,V)] :: RDD[(K,VV)] ::  ...
      // et permet d'extraire le type ABC : Seq[V] :: Seq[VV] :: ...
      checkCoGroup: CheckCogroup.Aux[K, H1 :: HRest, ABC],
      // permet de passer de ABC au tuple R (Seq[V], Seq[VV], ... )
      tupler: Tupler.Aux[ABC, R],
      // permet d'extraire ABC depuis un Array[Iterable[_]]
      fromTraversable: FromTraversable[ABC]): CoGroupN.Aux[K, H1 :: HRest, (K, R)] = new CoGroupN[K, H1 :: HRest] {
    type Res = (K, R)

    override def arrayToRes(k: K, array: Array[Iterable[_]]): Res =
      (k, tupler.apply(fromTraversable(array.map(_.toSeq)).get))

    override def toSeq(h: ::[H1, HRest]): List[RDD[(K, _)]] =
      checkCoGroup.toSeq(h)
  }

  /*
    Usage : cogroupN(rdd1 :: rdd2 :: rdd3 :: HNil)
   */
  def cogroupN[K, H <: HList, Res](h: H, part: Partitioner = new HashPartitioner(1024))(
      implicit
      ctK:   ClassTag[K],
      ctRes: ClassTag[Res],
      //ToTraversable.Aux[H,List,RDD[(K,_)]] don't work
      coGroupAble: CoGroupN.Aux[K, H, Res]
  ): RDD[Res] =
    new CoGroupedRDD(coGroupAble.toSeq(h), part)
      .map((coGroupAble.arrayToRes _).tupled)

}

object CoGroupX {

  def main(args: Array[String]): Unit = {
    val ss =
      SparkSession.builder().appName("toto").master("local[*]").getOrCreate()

    val d = ss.sparkContext.makeRDD(Seq("a" -> "b", "a" -> "c", "b" -> "d"))

    val f = d.mapValues(_ => 1)

    val n: RDD[(String, (Seq[String], Seq[String], Seq[String]))] =
      CoGroupN.cogroupN(d :: d :: d :: HNil)

    n.collect().foreach(println)

    val n1: RDD[(String, (Seq[String], Seq[String]))] =
      CoGroupN.cogroupN(d :: d :: HNil)

    val n2: RDD[(String, Seq[String])] = CoGroupN.cogroupN(d :: HNil)

    val n3: RDD[(String, (Seq[String], Seq[Int], Seq[Int], Seq[String]))] =
      CoGroupN.cogroupN(d :: f :: f :: d :: HNil)

    n3.collect.foreach(println)
  }

}
