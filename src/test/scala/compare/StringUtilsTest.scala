package compare

import io.univalence.utils.StringUtils
import org.scalatest.FunSuite

class StringUtilsTest extends FunSuite {

  test("letterPairs") {
    assert(StringUtils.letterPairs("abc").toSeq == Seq("ab", "bc"))
  }

}
