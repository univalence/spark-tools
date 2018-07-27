package io.univalence.utils

object StringUtils {
  def compareStrings(a: String, b: String): Double = {

    // identical strings
    if (a == b) return 1

    val pairs1: Seq[String] = wordLetterPairs(a)
    val pairs2: Seq[String] = wordLetterPairs(b)

    val union = pairs1.length + pairs2.length

    def frequencies[T](s1: Seq[T]): Map[T, Int] =
      s1.groupBy(identity).mapValues(_.size)

    def joinMap[K, V](m1: Map[K, V],
                      m2: Map[K, V]): Map[K, (Option[V], Option[V])] = {
      //todo : optimize
      (m1.keySet ++ m2.keySet).toSeq.map(k ⇒ k → (m1.get(k), m2.get(k))).toMap
    }

    val inter = joinMap(frequencies(pairs1), frequencies(pairs2)).values
      .map({
        case (Some(l), Some(r)) ⇒ Math.min(l, r)
        case _ ⇒ 0
      })
      .sum

    inter.toDouble * 2 / union
  }

  def letterPairs(str: String): Seq[String] = {
    str.sliding(2, 1).toSeq
  }

  def wordLetterPairs(str: String): Seq[String] = {
    str.split("\\s").flatMap(letterPairs)
  }
}
