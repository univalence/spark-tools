package io.univalence.parka

import com.twitter.algebird.QTree

object Histogram {
  case class LongHisto(neg: Option[QTree[Unit]], countZero:Long, pos: Option[QTree[Unit]]){
    def count: Long = pos.map(_.count).getOrElse(0L) + countZero + neg.map(_.count).getOrElse(0L)

    def quantileBounds(p: Double): (Double, Double) = {
      require(p >= 0.0 && p <= 1.0, "The given percentile must be of the form 0 <= p <= 1.0")
      (pos, neg) match {
        //case (Some(po), None) if countZero == 0 => po.quantileBounds(p)
        case (Some(po), None) =>
          val portion = p * (countZero + po.count)
          if (portion < countZero) (0.0, 0.0)
          else po.quantileBounds((portion - countZero) / po.count)
        /*case (None, Some(n)) if countZero == 0 =>
          val (l,u) = n.quantileBounds(1-p)
          (-u, -l)
         */
        case (None, Some(n)) => {
          val portion = p * (countZero + n.count)
          if (portion > countZero) (0.0, 0.0)
          else n.quantileBounds(1 - (portion - countZero) / n.count)
        }
        case (Some(po), Some(n)) =>
          val totalCount = this.count
          val portion = p * totalCount
          /*
          if (portion > n.count) {
            val posP = portion / po.count +  n.count
            po.quantileBounds(posP)
          } else {
            val negP = portion / n.count
            val (l,u) = n.quantileBounds(1 - negP)
            (-u, -l)
          }
          // -1 -2
         */

          // TODO : @Jon tu pourras vérifier stp
          if (portion < n.count) {
            val negP = portion / n.count
            val (l,u) = n.quantileBounds(1 - negP)
            (-u, -l)
          } else if (portion > n.count && portion < n.count + countZero) {
            (0.0, 0.0)
          } else {
            val posP = portion / (po.count + countZero + n.count)
            po.quantileBounds(posP)
          }
      }
    }
  }

  object LongHisto {
    def empty: LongHisto = LongHisto(neg = None, countZero = 0, pos = None)

    def value(x: Long): LongHisto =
      if (x == 0)
        LongHisto(None,1,None)
      else if (x < 0)
        LongHisto(neg = Some(QTree.value(-x)), countZero = 0, pos = None)
      else
        LongHisto(neg = None, countZero = 0, pos = Some(QTree.value(x)))
    

    //https://github.com/glamp/bashplotlib
    def asciiDisplayFromDylan(longHisto: LongHisto): String = ???
  }
}
