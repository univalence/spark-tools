package io.univalence

package object strings {
  implicit class KeyHelper(val sc: StringContext) extends AnyVal {
    def key(args: Key*): Key = macro KeyMacro.keyMacro

    //TODO : replace with macro
    def name(args: Nothing*): String with FieldName = FieldKey.createName(sc.raw()).get

    def index(args: Nothing*): Index = Index.create(sc.raw()).get
  }

}
