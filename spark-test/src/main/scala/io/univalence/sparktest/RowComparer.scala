package io.univalence.sparktest

import org.apache.spark.sql.Row
import java.sql.Timestamp
import scala.math.abs

object RowComparer {

  /** Approximate equality, based on equals from [[Row]] */
  def areRowsEqual(r1: Row, r2: Row, tol: Double): Boolean =
    if (r1.length == r2.length) {
      val colComp =
        for (idx <- 0 until r1.length)
          yield areColumnsEqual(r1, r2, tol, idx)

      colComp.forall(identity)
    } else {
      false
    }

  private def areColumnsEqual(r1: Row, r2: Row, tol: Double, idx: Int): Boolean =
    if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
      false
    } else if (!r1.isNullAt(idx)) {
      val o1 = r1.get(idx)
      val o2 = r2.get(idx)

      o1 match {
        case b1: Array[Byte] =>
          java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])

        case f1: Float =>
          (!(java.lang.Float.isNaN(f1) || java.lang.Float.isNaN(o2.asInstanceOf[Float]))
            && (abs(f1 - o2.asInstanceOf[Float]) <= tol))

        case d1: Double =>
          (!(java.lang.Double.isNaN(d1) || java.lang.Double.isNaN(o2.asInstanceOf[Double]))
            && abs(d1 - o2.asInstanceOf[Double]) <= tol)

        case d1: java.math.BigDecimal =>
          d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) == 0

        case t1: Timestamp =>
          abs(t1.getTime - o2.asInstanceOf[Timestamp].getTime) <= tol

        case _ => o1 == o2
      }
    } else true
}
