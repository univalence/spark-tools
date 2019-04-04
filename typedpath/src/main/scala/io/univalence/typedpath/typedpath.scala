package io.univalence

package object typedpath {
  implicit class PathHelper(val sc: StringContext) extends AnyVal {
    def path(args: Path*): Path = macro PathMacro.pathMacro
  }

}
