import io.univalence.utils.StringUtils
import org.scalatest.FunSuite


class StringUtilsTest extends FunSuite {


  test("letterPairs") {

    assert(StringUtils.letterPairs("abc").toSeq == Seq("ab","bc"))

    println("䣚".size)
    println(StringUtils.letterPairs("䣚").toSeq)
  }



}