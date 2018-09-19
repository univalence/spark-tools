package io.univalence.sparktools.kpialgebra

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import shapeless.contrib.spire._
import spire.algebra._
import spire.implicits._

import scala.reflect.ClassTag

case class DeltaPart[T: AdditiveMonoid](
    count: Long,
    part:  T
)

case class DeltaCommon[T: AdditiveMonoid](
    count:     Long,
    countZero: Long,
    diff:      T,
    error:     T,
    left:      T,
    right:     T
)

case class Delta[L: AdditiveMonoid, R: AdditiveMonoid, C: AdditiveMonoid](
    left:   DeltaPart[L],
    right:  DeltaPart[R],
    common: DeltaCommon[C]
)

object KpiAlgebra {

  def computeCommon[LRC: AdditiveAbGroup: MultiplicativeSemigroup](left: LRC, right: LRC): DeltaCommon[LRC] = {
    val diff  = left - right
    val error = diff * diff
    DeltaCommon(count     = 1,
                countZero = if (diff == Monoid.additive[LRC].id) 1 else 0,
                diff      = diff,
                error     = error,
                left      = left,
                right     = right)
  }

  def monoid[LM: AdditiveMonoid, RM: AdditiveMonoid, LRC: AdditiveMonoid]: Monoid[Delta[LM, RM, LRC]] =
    Monoid.additive[Delta[LM, RM, LRC]]

  def compare[K:   ClassTag,
              L:   ClassTag,
              R:   ClassTag,
              LM:  AdditiveMonoid: ClassTag,
              RM:  AdditiveMonoid: ClassTag,
              LRC: AdditiveAbGroup: MultiplicativeSemigroup: ClassTag](
      left:  RDD[(K, L)],
      right: RDD[(K, R)])(flm: L ⇒ LM, frm: R ⇒ RM, flc: L ⇒ LRC, frc: R ⇒ LRC): Delta[LM, RM, LRC] = {

    val map: RDD[Delta[LM, RM, LRC]] = left
      .fullOuterJoin(right)
      .map({
        case (_, (Some(l), None)) ⇒
          monoid[LM, RM, LRC].id
            .copy(left = DeltaPart(count = 1, part = flm(l)))
        case (_, (None, Some(r))) ⇒
          monoid[LM, RM, LRC].id
            .copy(right = DeltaPart(count = 1, part = frm(r)))
        case (_, (Some(l), Some(r))) ⇒
          monoid[LM, RM, LRC].id.copy(common = computeCommon(flc(l), frc(r)))
      })

    map.reduce((x, y) ⇒ monoid[LM, RM, LRC].op(x, y))
  }
}

case class KpiLeaf(l1: Long, l2: Long, l3: Long)

object KpiAlgebraTest {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("smoketest"))

    val parallelize: RDD[(Int, Int)] = sc.parallelize((1 to 4).zipWithIndex)

    /*println(((
      KpiAlgebra.compare(
        left = parallelize,
        right = parallelize
      )(
        flm = identity,
        frm = identity,
        flc = identity,
        frc = identity
      )
    )*/

    // Delta(DeltaPart(0,0),DeltaPart(0,0),DeltaCommon(4,4,0,0,6,6))

    val p2: RDD[(Int, KpiLeaf)] =
      sc.parallelize((1 to 4)).map(_ → KpiLeaf(1, 2, 3))

    import spire.implicits._
    import shapeless.contrib.spire._

    ////println(((KpiAlgebra.compare(p2, p2)(identity, identity, identity, identity))

  }
}
