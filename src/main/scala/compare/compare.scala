
def compareStrings(a: String, b: String): Int = {
  val strATrimUpper = a.trim.toUpperCase
  val strBTrimUpper = b.trim.toUpperCase

  // identical strings
  if (a == b) 1

  // one empty string
  if (a.isEmpty || b.isEmpty) 0

  // both one char
  if (a.length == 1 && b.length == 1) 0

  // one string is one char
  /*if (a.length == 1) return b.indexOf(a) > -1 ? 1 / b.length : 0;
  if (b.length == 1) return a.indexOf(b) > -1 ? 1 / a.length : 0;*/

  val pairs1 = wordLetterPairs(strATrimUpper)
  val pairs2 = wordLetterPairs(strBTrimUpper)

  val union = pairs1.length + pairs2.length

  val inter = (pairs1 intersect pairs2).length

  inter * 2 / union
}

def letterPairsLoop(listChar: List[Char],startIndex: Int, endIndex: Int): String = {
  if (endIndex >= listChar.length - 1) s"${listChar(startIndex)}${listChar(endIndex)}"
  else s"${listChar(startIndex)}${listChar(endIndex)}" + "," + letterPairsLoop(listChar, startIndex + 1, endIndex + 1)
}

def letterPairs(str: String): Array[String] = {
  val listChar = str.toList
  letterPairsLoop(listChar,0,1).split(",")
}

def wordLetterPairs(str: String): Array[String] = {
  val listChar = str.trim.toList
  letterPairsLoop(listChar,0,1).split(",")
}
