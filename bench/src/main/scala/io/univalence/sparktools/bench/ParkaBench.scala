package io.univalence.sparktools.bench

import cats.kernel.Monoid
import io.univalence.parka.MonoidGen
import org.openjdk.jmh.annotations._

import scala.collection.mutable
import scala.reflect.ClassTag

object MapMonoidBench {

  @State(Scope.Benchmark)
  abstract class AbstractBenchmark(val maps: Maps, val monoid: Monoid[Map[String, Int]]) {

    @Benchmark
    def m1_and_m2(): Unit =
      monoid.combine(maps.m1, maps.m2)

    @Benchmark
    def times2(): Unit =
      monoid.combine(maps.m1, maps.m1)

  }

  trait Maps {
    val m1: Map[String, Int]
    val m2: Map[String, Int]
  }
  def text(string: String): Map[String, Int] =
    string.split(' ').groupBy(x => x).mapValues(_.length).toMap

  object map1 extends Maps {
    val m1: Map[String, Int] = text("abc abc def def ghi def jkl mno")
    val m2: Map[String, Int] = text("def def ghi def jkl mno mno")
  }

  object map2 extends Maps {
    val m1: ColBasedMap[String, Int] = ColBasedMap.toColbaseMap(map1.m1)
    val m2: ColBasedMap[String, Int] = ColBasedMap.toColbaseMap(map1.m2)
  }

  import com.twitter.algebird.Ring._
  val monoid2: Monoid[Map[String, Int]] = MapMonoid.mapMonoid2
  val monoid1: Monoid[Map[String, Int]] = MapMonoid.mapMonoidInit
  val monoid3: Monoid[Map[String, Int]] = MapMonoid.mapMonoid3

  class Map1Monoid1 extends AbstractBenchmark(map1, monoid1)
  class Map1Monoid2 extends AbstractBenchmark(map1, monoid2)
  class Map2Monoid2 extends AbstractBenchmark(map2, monoid2)
  class Map2Monoid3 extends AbstractBenchmark(map2, monoid3)

}

object MapMonoid {

  def mapMonoidInit[K, V: Monoid]: Monoid[Map[K, V]] =
    MonoidGen(
      Map.empty,
      (m1, m2) => {
        (m1.keySet ++ m2.keySet)
          .map(
            k =>
              k -> (m1.get(k) match {
                case None => m2.getOrElse(k, Monoid[V].empty)
                case Some(x) =>
                  m2.get(k) match {
                    case Some(y) => Monoid[V].combine(x, y)
                    case None    => x
                  }
              })
          )
          .toMap
      }
    )

  def mapMonoid2[K, V: Monoid]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    @inline
    override def empty: Map[K, V] = Map.empty

    @inline
    override def combine(m1: Map[K, V], m2: Map[K, V]): Map[K, V] = {
      val m1k = m1.keySet
      m1.map({
        case (k, v) =>
          (k, m2.get(k).fold(v)(v2 => Monoid[V].combine(v, v2)))
      }) ++ m2.filterKeys(k => !m1k(k))
    }
  }

  def mapMonoid3[K, V: Monoid: ClassTag]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {

    val valueMonoid: Monoid[V] = Monoid[V]

    val proxiedMonoid: Monoid[Map[K, V]] = mapMonoid2[K, V]

    override def empty: Map[K, V] = ColBasedMap.empty

    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] =
      (x, y) match {
        case (cx: ColBasedMap[K, V], cy: ColBasedMap[K, V]) => cx.combine(cy)
        case _                                              => proxiedMonoid.combine(x, y)
      }
  }
}

class ColBasedMap[K, V] private (val keys_ : Array[Any], val values_ : Array[Any]) extends Map[K, V] {

  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] =
    keys_.indexOf(kv._1) match {
      case -1 => new ColBasedMap(keys_ :+ kv._1, values_ :+ kv._2)
      case i  => new ColBasedMap(keys_, values_.updated(i, kv._2))
    }

  override def get(key: K): Option[V] =
    keys_.indexOf(key) match {
      case -1 => None
      case i  => Some(values_(i).asInstanceOf[V])
    }

  override def iterator: Iterator[(K, V)] = keys_.zip(values_).toIterator.asInstanceOf[Iterator[(K, V)]]

  override def -(key: K): ColBasedMap[K, V] =
    keys_.indexOf(key) match {
      case -1 => this
      case i =>
        import ColBasedMap._
        new ColBasedMap(removeIndex(keys_, i), removeIndex(values_, i))
    }

  def combine(right: ColBasedMap[K, V])(implicit monoid: Monoid[V]): ColBasedMap[K, V] =
    if (keys_ sameElements right.keys_) {
      val arr: Array[Any] = new Array(values_.length)
      for (i <- arr.indices) {
        arr(i) = monoid.combine(values_(i).asInstanceOf[V], right.values_(i).asInstanceOf[V])
      }
      new ColBasedMap(keys_, arr)
    } else {

      if (right.values_.length > values_.length) {
        right.combine(this)
      } else {

        val indexMap: Array[Int] = new Array(right.values_.length)
        var extraValue: Int      = 0

        for (i <- right.keys_.indices) {
          keys_.indexOf(right.keys_(i)) match {
            case -1 =>
              extraValue += 1
              indexMap(i) = values_.length + extraValue
            case x =>
              indexMap(i) = x
          }
        }

        val resKeys: Array[Any]   = new Array(keys_.length + extraValue)
        val resValues: Array[Any] = new Array(keys_.length + extraValue)

        keys_.copyToArray(resKeys)
        values_.copyToArray(resValues)

        for (i <- indexMap.indices) {
          val j = indexMap(i)
          resKeys(j)   = right.keys_(i)
          resValues(j) = monoid.combine(values_(j).asInstanceOf[V], right.values_(i).asInstanceOf[V])
        }

        new ColBasedMap(resKeys, resValues)
      }

    }
}

object ColBasedMap {

  def empty[K, V]: ColBasedMap[K, V] = new ColBasedMap[K, V](Array.empty, Array.empty)

  def toColbaseMap[K, V](map: Map[K, V]): ColBasedMap[K, V] = {
    val kvs = map.toArray
    new ColBasedMap(kvs.map(_._1).toArray, kvs.map(_._2).toArray)
  }

  def removeIndex[E: ClassTag](seq: Array[E], index: Int): Array[E] =
    (seq.take(index) ++ seq.drop(index + 1)).toArray

  def apply[K, V](keys_ : Seq[K], values_ : Seq[V]): ColBasedMap[K, V] =
    new ColBasedMap(keys_.toArray, values_.toArray)
}

object App {

  def main(args: Array[String]): Unit =
    new MapMonoidBench.Map2Monoid3().m1_and_m2()

}
