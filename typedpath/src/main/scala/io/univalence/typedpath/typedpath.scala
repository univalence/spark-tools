package io.univalence

package object typedpath {
  implicit class PathHelper(val sc: StringContext) extends AnyVal {
    def path(args: Path*): Path = macro PathMacro.pathMacro

    //TODO : replace with macro
    def name(args: Nothing*): String with FieldName = FieldPath.createName(sc.raw()).get

    def index(args: Nothing*): Index = Index.create(sc.raw()).get
  }

}
