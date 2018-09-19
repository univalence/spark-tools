package compare

import io.univalence.utils.StringUtils
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop._

object StringSpecification extends Properties("StringUtils") {

  private val asciiLetter: Gen[Char] =
    Gen.asciiPrintableChar.retryUntil(_.isLetter, 1000)

  private val asciiLetterString: Gen[String] =
    Gen.listOf(asciiLetter).map(_.mkString)

  def isAsciiLetter(c: Char): Boolean = c.isLetter && c <= 127

  property("letterPairs") = forAll(asciiLetterString) { a: String ⇒
    (a.size > 1) ==>
      (StringUtils
      .letterPairs(a)
      .map(_.head)
      .mkString == a.dropRight(1))
  }

  property("compareStrings should be 1 for identical strings") = forAll { a: String ⇒
    StringUtils.compareStrings(a, a) == 1
  }

  property("compareStrings") = forAll { (a: String, b: String) ⇒
    val result = StringUtils.compareStrings(a, b)

    (a != b) ==> (result < 1 && result >= 0)
  }

}
