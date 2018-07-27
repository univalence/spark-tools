import io.univalence.utils
import io.univalence.utils.StringUtils
import org.scalacheck.Properties
import org.scalacheck.Prop._

object StringSpecification extends Properties("String") {

  def isAsciiLetter(c: Char) = c.isLetter && c <= 127

  /*property("letterPairs") = forAll { (a:String) =>
    //TODO Gen Ascii
    a.forall(isAsciiLetter) ==> (StringUtils.letterPairs(a).toSeq.sliding(2,2).map(_.head).mkString == a.dropRight(1))
  }*/

  property("compareStrings") = forAll { (a: String) ⇒
    StringUtils.compareStrings(a, a) == 1
  }

  property("compareStrings2") = forAll { (a: String, b: String) ⇒
    (a != b) ==> (StringUtils.compareStrings(a, b) < 1 && utils.StringUtils
      .compareStrings(a, b) >= 0)
  }

}
