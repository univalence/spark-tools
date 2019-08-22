package io.univalence.parka

import cats.kernel.Semigroup

import scala.collection.immutable.BitSet

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

        val ksToInts: Set[K] => BitSet = set => set.map(keys.indexOf).foldLeft(BitSet.empty)(_ + _)

        val model = KModes2.fit(map.keys.map(ksToInts).map(x => 1.0 -> x).toVector, maxSize, 20)

        map.toSeq
          .groupBy(row => model.predict(ksToInts(row._1)))
          .map(x => {
            val xs    = x._2
            val key   = xs.map(_._1).reduce(_ ++ _)
            val value = xs.map(_._2).reduce(semi.combine)

            key -> value
          })
      }

    }
}

case class KModes2(centers: Vector[BitSet]) {
  def findKeyset(point: BitSet): Option[(Double, Int)] =
    centers.zipWithIndex
      .collect({
        case (x, i) if point.forall(x) =>
          (x.count(x => !point(x)).toDouble, i)
      })
      .sortBy(_._1)
      .headOption

  def predict(point: BitSet): Int = nearest(point)._2

  def nearest(bitSet: BitSet): (Double, Int) =
    centers.zipWithIndex
      .map({
        case (x, i) =>
          val core = x.count(bitSet)
          val n2   = x.size
          val n4   = bitSet.size

          val distance = ((n2 - core) + (n4 - core)) * (1 + Math.sqrt(n2 + n4 + core))

          (distance.toDouble, i)
      })
      .minBy(_._1)
}

object KModes2 {

  def iterate(kModes2: KModes2, data: Vector[(Double, BitSet)], dim: Int): KModes2 = {
    val k = kModes2.centers.size

    val nearest_0: Vector[(Double, Int)] = data.map(d => kModes2.findKeyset(d._2).getOrElse(kModes2.nearest(d._2)))

    val wcss: Double   = nearest_0.map(_._1).sum
    val y: Vector[Int] = nearest_0.map(_._2)

    case class State(size: Vector[Int], modes: Vector[Vector[Int]])
    val zero: State = State(Vector.fill(k)(0), Vector.fill(k, dim)(0))

    type Elem = ((Double, BitSet), Int)
    def nextState(state: State, elem: Elem): State = {

      val i2 = y(elem._2)

      val modes = state.modes.updated(i2,
                                      state
                                        .modes(i2)
                                        .zipWithIndex
                                        .map({
                                          case (x, j) if elem._1._2(j) => x + 1
                                          case (x, _)                  => x
                                        }))

      State(size = state.size.updated(i2, state.size(i2) + 1), modes = modes)
    }

    val state: State = data.zipWithIndex.foldLeft(zero)(nextState)

    val centers: Vector[BitSet] = state.size
      .zip(state.modes)
      .map({
        case (0, m) => randBitSet(dim)
        case (s, m) =>
          m.zipWithIndex
            .collect({
              case (n, i) if n * 4 >= s => i
            })
            .foldLeft(BitSet.empty)({
              case (b, i) => b + i
            })
      })

    KModes2(centers)
  }

  def newKModes2(k: Int, dim: Int): KModes2 =
    KModes2(
      (0 until k)
        .map(_ => {
          randBitSet(dim)
        })
        .toVector
    )

  private def randBitSet(dim: Int): BitSet = {
    val int = scala.util.Random.nextInt(Math.pow(2, dim).intValue)
    BitSet.fromBitMaskNoCopy(Array(int.toLong))
  }

  def fit(data: Vector[(Double, BitSet)], k: Int, maxIteration: Int): KModes2 = {
    require(data.nonEmpty)

    def maxBitSet(bitSet: BitSet): Int =
      bitSet.max

    val dim: Int = data.map(_._2).map(maxBitSet).max + 1

    (0 until maxIteration).foldLeft(newKModes2(k, dim))({
      case (kmodes2, _) =>
        iterate(kmodes2, data, dim)
    })
  }

}
