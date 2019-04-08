package io.univalence.plumbus

import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset}

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

  implicit class KVGD[K,A](val kvgd:KeyValueGroupedDataset[K,A]) {
    def cogroup[B](right:KeyValueGroupedDataset[K,B]):Dataset[(K,Seq[A],Seq[B])] = {
      //Use SparkAddOn ?
      ???
    }
  }

  def apply[A,B,K](left:Dataset[A],right: Dataset[B])(keyLeft:A => K, keyRight:B => K)(encA:Encoder[A],encB:Encoder[B],encC:Encoder[K] /* ... */):Dataset[(K,Seq[A],Seq[B])] = ???
}
