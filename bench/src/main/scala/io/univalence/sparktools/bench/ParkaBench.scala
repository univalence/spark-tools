package io.univalence.sparktools.bench

import cats.kernel.Monoid
import io.univalence.parka.{ MonoidGen, RowBasedMap }
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
    val m1: RowBasedMap[String, Int] = RowBasedMap.toColbaseMap(map1.m1)
    val m2: RowBasedMap[String, Int] = RowBasedMap.toColbaseMap(map1.m2)
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

  def mapMonoid3[K, V: Monoid]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {

    val valueMonoid: Monoid[V] = Monoid[V]

    val proxiedMonoid: Monoid[Map[K, V]] = mapMonoid2[K, V]

    override def empty: Map[K, V] = RowBasedMap.empty

    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] =
      (x, y) match {
        case (cx: RowBasedMap[K, V], cy: RowBasedMap[K, V]) => cx.combine(cy)
        case _                                              => proxiedMonoid.combine(x, y)
      }
  }
}

object App {

  def main(args: Array[String]): Unit =
    new MapMonoidBench.Map2Monoid3().m1_and_m2()

}
