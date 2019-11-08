package io.univalence.parka

import cats.kernel.Monoid

import scala.reflect.ClassTag

/**
  * Implementation Note
  * It uses Array[Any] as {@link scala.collection.mutable.ArraySeq}
  * **/
class RowBasedMap[K, V] private (val keys_ : Array[Any], val values_ : Array[Any]) extends Map[K, V] with Serializable {

  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] =
    keys_.indexOf(kv._1) match {
      case -1 => new RowBasedMap(keys_ :+ kv._1, values_ :+ kv._2)
      case i  => new RowBasedMap(keys_, values_.updated(i, kv._2))
    }

  override def get(key: K): Option[V] =
    keys_.indexOf(key) match {
      case -1 => None
      case i  => Some(values_(i).asInstanceOf[V])
    }

  override def iterator: Iterator[(K, V)] = keys_.zip(values_).toIterator.asInstanceOf[Iterator[(K, V)]]

  override def -(key: K): RowBasedMap[K, V] =
    keys_.indexOf(key) match {
      case -1 => this
      case i =>
        import RowBasedMap._
        new RowBasedMap(removeIndex(keys_, i), removeIndex(values_, i))
    }

  def combine(right: RowBasedMap[K, V])(implicit monoid: Monoid[V]): RowBasedMap[K, V] =
    if (keys_ sameElements right.keys_) {
      val arr: Array[Any] = new Array(values_.length)
      for (i <- arr.indices) {
        arr(i) = monoid.combine(values_(i).asInstanceOf[V], right.values_(i).asInstanceOf[V])
      }
      new RowBasedMap(keys_, arr)
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

        new RowBasedMap(resKeys, resValues)
      }

    }

}

object RowBasedMap {

  def empty[K, V]: RowBasedMap[K, V] = new RowBasedMap[K, V](Array.empty, Array.empty)

  implicit def toColbaseMap[K, V](map: Map[K, V]): RowBasedMap[K, V] = {
    val kvs = map.toArray
    new RowBasedMap(kvs.map(_._1).toArray, kvs.map(_._2).toArray)
  }

  def toColbaseMapFromSeq[K, V](seq: Seq[(K, V)]): RowBasedMap[K, V] =
    new RowBasedMap(seq.map(_._1).toArray, seq.map(_._2).toArray)

  def removeIndex[E: ClassTag](seq: Array[E], index: Int): Array[E] =
    (seq.take(index) ++ seq.drop(index + 1)).toArray

  def apply[K, V](keys_ : Seq[K], values_ : Seq[V]): RowBasedMap[K, V] =
    new RowBasedMap(keys_.toArray, values_.toArray)
}
