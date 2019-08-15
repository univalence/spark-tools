package io.univalence.parka

import cats.kernel.Semigroup
import org.clustering4ever.clustering.kcenters.scala.{ KModes, KModesModel }
import org.clustering4ever.math.distances.binary.Hamming

object CompressMap {

  def apply[K, V: Semigroup](map: Map[Set[K], V], maxSize: Int): Map[Set[K], V] =
    if (maxSize >= map.size) map
    else {
      assert(maxSize > 0)
      val semi = implicitly[Semigroup[V]]

      if (maxSize == 1) {

        if (map.isEmpty)
          Map.empty
        else {
          val value        = semi.combineAllOption(map.values).get
          val keys: Set[K] = map.keys.reduce(_ ++ _)
          Map(keys -> value)
        }
      } else {
        val keys: Vector[K] = map.keys.reduce(_ ++ _).toVector

        val ksToInts: Set[K] => Array[Int] = set => keys.map(k => if (set(k)) 1 else 0).toArray

        val model = KModes.fit(map.keys.map(ksToInts).toVector, maxSize, Hamming(), 50, 3)

        map.toSeq
          .groupBy(row => model.centerPredict(ksToInts(row._1)))
          .map(x => {
            val xs    = x._2
            val key   = xs.map(_._1).reduce(_ ++ _)
            val value = xs.map(_._2).reduce(semi.combine)

            key -> value
          })
      }

    }

}
