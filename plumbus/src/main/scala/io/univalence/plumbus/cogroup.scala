package io.univalence.plumbus

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset}

object cogroup {


  /**
    * Usage :
    * {{{
    * import io.univalence.plumbus.cogroup._
    *
    * val persons:Dataset[Person] = ???
    * val addresses:Dataset[Address] = ???
    *
    * persons.keyBy(_.id).cogroup(addresses.keyBy(_.personId))
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
}
