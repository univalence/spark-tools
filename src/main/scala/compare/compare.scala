import shapeless.T

def compareStrings(a: String, b: String): Int = {
  val strATrimUpper = a.trim.toUpperCase
  val strBTrimUpper = b.trim.toUpperCase

  // identical strings
  if (a == b) 1
  // one empty string
  // issue with operator ! and ||
  if (a != b) 0
  // both one char
  if (a.length == 1 && b.length == 1) 0
  // one string is one char
  0
}

def letterPairs(listChar: List[Char],startIndex: Int, endIndex: Int): String = {
  if (endIndex >= listChar.length - 1) s"${listChar(startIndex)}${listChar(endIndex)}"
  else s"${listChar(startIndex)}${listChar(endIndex)}" + "," + letterPairs(listChar, startIndex + 1, endIndex + 1)
}

val lc = "abcdef".toList

//letterPairs(l,0,1)

def yolo(listChar: List[Char]): String = letterPairs(listChar,0,1)

val a = yolo(lc).split(",")

//def wordLetterPairs

def flatten(arr: Array[T]): Unit = {

}