package io.univalence.autobuild

import shapeless.CaseClassMacros

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros._

trait TypeName[T] {
  def name: String
}

object TypeName {

  import scala.reflect.runtime.universe.TypeTag

  implicit def fromTypeTag[T](implicit typeTag: TypeTag[T]): TypeName[T] =
    new TypeName[T] {
      override def name: String = typeTag.tpe.toString
    }
}

object MacroMarker {
  def generated_applicative: Nothing =
    throw new NotImplementedError("should not exist at runtime")
}

object CaseClassApplicativeBuilder {

  case class Generated(declaration: String, impl: String) {
    def all: String = declaration + " = " + impl
  }

  def generateDef(fieldNames: List[(String, String)], name: String): Generated = {
    val signature: String = "def build(" + fieldNames
      .map(
        { case (n, t) ⇒ s"$n : Result[$t]" }
      )
      .mkString(",\n") + "):Result[" + name + "]"

    val map: List[(String, Int, String)] =
      fieldNames.zipWithIndex.map(t ⇒ (t._1._1, t._2 + 1, t._1._2))
    val vals: String = map
      .map({ case (n, i, _) ⇒ s"""val _$i = $n.addPathPart("$n")""" })
      .mkString("\n")

    val allResults: String = "List(" + map
      .map({ case (n, i, _) ⇒ "_" + i })
      .mkString(",") + ")"

    Generated(
      signature,
      s"""{
    $vals
    val allResults:Vector[Result[Any]] = Vector(${map
        .map({ case (_, i, _) ⇒ "_" + i })
        .mkString(",")})
    val allAnnotations = allResults.flatMap(_.annotations)
    if(allResults.forall(_.value.isDefined)) {
      Result(Some($name(${map
        .map({ case (n, i, _) ⇒ s"$n = _$i.value.get" })
        .mkString(",\n")})),allAnnotations)
    } else {
       val missingFields = allResults.zip(List(${map
        .map({ case (n, _, t) ⇒ "(\"" + n + "\",\"" + t + "\")" })
        .mkString(",")})).filter(!_._1.value.isDefined).map(_._2)
       import io.univalence.centrifuge.Annotation
       val missingFieldsAnnotations = missingFields.map(f => Annotation.missingField(f._1))
       Result(None,missingFieldsAnnotations ++ allAnnotations)
    }
    }"""
    )

  }

}

@compileTimeOnly("enable macro paradise to expand macro annotations")
class autoBuildResult extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any =
    macro AutoBuildConstruct.autoBuildResultImpl
}

object CleanTypeName {
  def clean(s: String): String = {
    s.replaceAll("([a-zA-Z@0-9]+\\.)+([a-zA-Z@0-9]+)", "$2")
      .replaceAll("@@\\[([a-zA-Z0-9]+) *, *([a-zA-Z0-9]+)\\]", "$1 @@ $2")
  }
}

class AutoBuildConstruct[C <: whitebox.Context](val c: C) extends CaseClassMacros {

  def autoBuildResultImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val inputs = annottees.map(_.tree).toList
    val annottee: DefDef = inputs match {
      case (param: DefDef) :: Nil ⇒ param
    }

    def extractA(t: Tree): Name = {
      val AppliedTypeTree(Ident(nn), args) = t
      nn
    }

    //to try for position : https://github.com/kevinwright/macroflection/blob/master/kernel/src/main/scala/net/thecoda/macroflection/Validation.scala#L75

    def stringToType(s: String): Type = {
      val dummyNme: String = c.fresh()
      val parse                         = c.parse(s"{ type $dummyNme = " + s + " }")
      val q"{ type $dummyNme2 = $tpt }" = c.typeCheck(parse)
      tpt.tpe.asInstanceOf[Type] // helping IntelliJ
    }

    annottee match {

      case q"..$defMods def $name[..$tparams](...$paramss): $rTpe = $implBlock" ⇒ {

        rTpe match {
          case tq"Result[$t]" ⇒ {
            val typeToBuild = stringToType(t.toString)

            val of: List[(TermName, Type)] = fieldsOf(typeToBuild)

            val argSpec: List[(TermName, Type)] = paramss.headOption
              .getOrElse(Nil)
              .asInstanceOf[List[ValDef]]
              .map({
                case ValDef(_, vname, vtpt, _) ⇒ {
                  val inType = vtpt match {
                    case tq"Result[$paramInType]" ⇒ paramInType
                  }
                  vname → stringToType(inType.toString)
                }
              })

            val generateDef = CaseClassApplicativeBuilder.generateDef(
              of.map(t ⇒ t._1.encoded → CleanTypeName.clean(t._2.toString)),
              CleanTypeName.clean(typeToBuild.toString))

            if (of.toMap != argSpec.toMap) {
              c.abort(
                annottee.pos,
                "signature not matching to build " + typeToBuild.toString + ", use : \n\n @autoBuildResult\n " + generateDef.declaration + " = MacroMarker.generated_applicative"
              )
            }

            val result = c.parse(generateDef.all)
            c.Expr[Any](result)

          }
        }
      }
    }
  }
}

object AutoBuildConstruct {
  def autoBuildResultImpl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] =
    new AutoBuildConstruct[c.type](c).autoBuildResultImpl(annottees: _*)
}
