package net.ironforged.scaladiff

package object commons {
  /**
   * Insert one or more elements within a list
   * @param xs The target list
   * @param index The index at which to insert the new element
   * @param elements The list of elements to insert
   * @tparam T The type of the list
   * @return List[T]
   */
  def insert[T](xs: List[T], index: Int)(elements: T*): List[T] = {
    xs.take(index) ++ elements ++ xs.drop(index)
  }

  /**
   * Replace one or more elements within a list with another set of elements
   * @param xs The target list
   * @param start The index to begin replacement
   * @param num The number of elements to replace
   * @param elements The elements to swap in
   * @tparam T The type of the list
   * @return List[T]
   */
  def replace[T](xs: List[T], start: Int, num: Int)(elements: T*): List[T] = {
    xs.take(start) ++ elements ++ xs.drop(start + num)
  }

  /**
   * A safe substring operation that starts at the provided index and ends either at the end of the string
   * or after `length` characters have been sliced
   * @param s The source string
   * @param start The index to start slicing at
   * @param length The number of characters to slice
   * @return String
   */
  def slice(s: String, start: Int, length: Int = Int.MinValue): String = {
    val len = if (length == Int.MinValue) s.length - start else length
    if (s == null)                    return ""
    else if (start < 0 || len < 0)    return ""
    else if (start > s.length)        return ""
    else if (len >= s.length - start) return s.substring(start)
    else                              s.substring(start, start + len)
  }

  /**
   * A safe substring operation that starts at the beginning of the string and selects towards the end
   * @param s The source string
   * @param x The number of characters to select
   * @return String
   */
  def sliceLeft(s: String, x: Int): String = {
    if (s == null)          return ""
    else if (x <= 0)        return ""
    else if (x > s.length)  return s
    else                    s.substring(0, x)
  }

  /**
   * A safe substring operation that starts at the end of the string and selects towards the beginning
   * @param s The source string
   * @param x The number of characters to select
   * @return String
   */
  def sliceRight(s: String, x: Int): String = {
    if (s == null)         return ""
    else if (x <= 0)       return ""
    else if (x > s.length) return s
    else                   s.substring(s.length - x)
  }
}
