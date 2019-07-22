package io.univalence.parka

import java.util.Optional
import org.jline.terminal.{ Size, TerminalBuilder }

case class ConsoleSize(columns: Int, rows: Int)

object ConsoleSize {

  val default = ConsoleSize(80, 25)

  def get: ConsoleSize =
    if (isInsideTrueTerminal) fromJline(TerminalBuilder.terminal().getSize)
    else default

  private def fromJline(size: Size) = ConsoleSize(size.getColumns, size.getRows)

  private def isInsideTrueTerminal: Boolean =
    (for {
      inIntelliJ <- isInsideIntelliJ
    } yield isInsideEmacs || !inIntelliJ).getOrElse(false)

  private def isInsideEmacs: Boolean = System.getenv("INSIDE_EMACS") != null

  /** Detect IntelliJ Idea.
    *
    * Code according to JLine.
    *
    * @return None if the current JVM version can't get the parent process. Some(true) if IntelliJ IDEA has been
    *         detected as parent process.
    */
  private def isInsideIntelliJ: Option[Boolean] =
    try {
      // java.lang.ProcessHandle is defined in Java 9+ and not in Java 8 or before. So reflexion is mandatory here.
      val phClass    = Class.forName("java.lang.ProcessHandle")
      val current    = phClass.getMethod("current").invoke(null)
      val parent     = phClass.getMethod("parent").invoke(current).asInstanceOf[Optional[AnyRef]].orElse(null)
      val infoMethod = phClass.getMethod("info")
      val info       = infoMethod.invoke(parent)
      val command =
        infoMethod.getReturnType.getMethod("command").invoke(info).asInstanceOf[Optional[String]].orElse(null)

      Some(command != null && command.contains("idea"))
    } catch {
      case _: Throwable => None
    }
}
