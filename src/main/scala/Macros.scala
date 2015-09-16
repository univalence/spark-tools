package io.univalence.autobuild

import struct.{TypeName, FieldsNonRecur}
import shapeless.CaseClassMacros

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros._






object CaseClassApplicativeBuilder {

  case class Generated(declaration: String, impl: String) {
    def all: String = declaration + " = " + impl
  }

  def builderFunSourceCode[A](implicit tmr: FieldsNonRecur[A], tv: TypeName[A]) = {
    val fieldnames1: List[(String, String)] = tmr.fieldnames

    val name: String = tv.name

    generateDef(fieldnames1, name).all
  }


  def generateDef2(fieldNames1:List[(String,String)], name:String): Generated = {
    val signature:String = "def build(" + fieldNames1.map({case (n,t) => s"$n : Result[$t]"}).mkString(",\n") + "):Result[" + name + "]"

    val map: List[(String, Int)] = fieldNames1.map(_._1).zipWithIndex.map(t => (t._1,t._2 + 1))

    val vals: String = map.map({case (n,i) => s"val _$i = $n.addPathPart(" + "\"" + n + "\")"}).mkString("\n")

    /*   Result(nominal = (_1.nominal, _2.nominal) match {
      case (Some(a), Some(b)) => Some(FixeRequeteInfo(a, b))
      case _ => None
    }, annotations = _1.annotations ::: _2.annotations) */

    val nominal: String = "(" + map.map(_._2).map("_" + _ + ".nominal").mkString(", ") + ") match {\n case (" + map.map(_._2).map("Some(s"+ _ +")").mkString(", ") + ") => Some(" + name + "(" + map.map({case (n,i) => n + " = s" + i}).mkString(", ") + ")) \n case _ => None }"

    val annotations:String = map.map(_._2).map("_" + _ + ".annotations").mkString(" ::: ")

    val result: String = s"Result(nominal = $nominal, annotations = $annotations )"

    Generated(signature, "{ \n\n " +  vals + "\n\n" + result + " \n}")

  }


  def generateDef(fieldnames1: List[(String, String)], name: String): Generated = {
    val signature: String = "def build[App[_]](" + fieldnames1.map({ case (n, t) => s"$n : App[$t]" }).mkString(",\n") + ")(implicit app:scalaz.Applicative[App],pathA:datalab.pj.core.PathAwareness[App]):App[" + name + "]"
    val injectPrefix: (String) => String = f => "pathA.injectPrefix(\"" + f + "\")(" + f + ")"

    val impl: String = if (fieldnames1.size < 12) {
      "import scalaz.syntax.apply._ \n" + fieldnames1.map(_._1).map(injectPrefix).mkString("(", " |@| ", ")") + s"($name.apply)"
    } else {
      val prefix = "import scalaz.syntax.apply._\n"
      val index: List[(List[(String, Int)], Int)] = fieldnames1.map(_._1).sliding(5, 5).toList.map(_.zipWithIndex).zipWithIndex
      val inApply: List[String] = for ((l, p1) <- index; (n, p2) <- l) yield {
        n + " = t._" + (p1 + 1) + "._" + (p2 + 1)
      }
      prefix + "app.tuple" + index.size + "(" +
        index.map(tuple => {
          val names: List[String] = tuple._1.map(_._1)
          "app.tuple" + names.size + "(" + names.map(injectPrefix).mkString(",") + ")"
        }).mkString(",\n") + ").map(t => " + name + ".apply(" + inApply.mkString(",\n") + "))"

    }

    Generated("//GENERATED AUTOMATICALY, DO NOT EDIT\n" + signature, "{\n" + impl + "}")
  }
}


@compileTimeOnly("enable macro paradise to expand macro annotations")
class autoApplicativeBuilder extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro ApplicativeKeyConstruct.autoApplicativeImpl
}


@compileTimeOnly("enable macro paradise to expand macro annotations")
class autoBuildResult extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro ApplicativeKeyConstruct.autoBuildResultImpl
}

object CleanTypeName {

  def clean(s:String):String =  {
    s.replaceAll("([a-zA-Z@0-9]+\\.)+([a-zA-Z@0-9]+)","$2").replaceAll("@@\\[([a-zA-Z0-9]+) *, *([a-zA-Z0-9]+)\\]","$1 @@ $2")
  }
}


class ApplicativeKeyConstruct[C <: Context](val c: C) extends CaseClassMacros {


  def autoBuildResultImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val inputs = annottees.map(_.tree).toList
    val annottee: DefDef = inputs match {
      case (param: DefDef) :: Nil => param
    }

    def extractA(t: Tree): Name = {
      val AppliedTypeTree(Ident(nn), args) = t
      nn
    }

    //to try for position : https://github.com/kevinwright/macroflection/blob/master/kernel/src/main/scala/net/thecoda/macroflection/Validation.scala#L75


    def stringToType(s: String): Type = {
      val dummyNme: String = c.fresh()
      val parse = c.parse(s"{ type $dummyNme = " + s + " }")
      val q"{ type $dummyNme2 = $tpt }" = c.typeCheck(parse)
      tpt.tpe

    }

    annottee match {

      case q"..$defMods def $name[..$tparams](...$paramss): $rTpe = $implBlock" => {

        rTpe match {
          case tq"Result[$t]" => {
            val typeToBuild = stringToType(t.toString)

            val of: List[(TermName, Type)] = fieldsOf(typeToBuild)

            val argSpec: List[(TermName, Type)] = paramss.headOption.getOrElse(Nil).map({ case ValDef(_, vname, vtpt, _) => {
              val inType = vtpt match {
                case tq"Result[$paramInType]" => paramInType
              }
              vname -> stringToType(inType.toString)
            }
            })


            val generateDef = CaseClassApplicativeBuilder.generateDef2(of.map(t => t._1.encoded ->  CleanTypeName.clean(t._2.toString)), CleanTypeName.clean(typeToBuild.toString))


            if (of.toMap != argSpec.toMap) {
              c.abort(annottee.pos, "signature not matching to build " + typeToBuild.toString + ", use : \n " + generateDef.all )
            }


            val result = c.parse(generateDef.all)

            c.Expr[Any](result)

          }
        }
      }
    }
  }


  def autoApplicativeImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val inputs = annottees.map(_.tree).toList
    val annottee: DefDef = inputs match {
      case (param: DefDef) :: Nil => param
    }

    def extractA(t: Tree): Name = {
      val AppliedTypeTree(Ident(nn), args) = t
      nn
    }

    //to try for position : https://github.com/kevinwright/macroflection/blob/master/kernel/src/main/scala/net/thecoda/macroflection/Validation.scala#L75


    def stringToType(s: String): Type = {
      val dummyNme: String = c.fresh()
      val parse = c.parse(s"{ type $dummyNme = " + s + " }")
      val q"{ type $dummyNme2 = $tpt }" = c.typeCheck(parse)
      tpt.tpe

    }

    annottee match {

      case q"..$defMods def $name[..$tparams](...$paramss): $rTpe = $implBlock" => {

        rTpe match {
          case tq"$a[$t]" => {
            val typeToBuild = stringToType(t.toString)

            val of: List[(TermName, Type)] = fieldsOf(typeToBuild)

            val argSpec: List[(TermName, Type)] = paramss.headOption.getOrElse(Nil).map({ case ValDef(_, vname, vtpt, _) => {
              val inType = vtpt match {
                case tq"$aa[$paramInType]" if extractA(vtpt) == extractA(rTpe) => paramInType
              }
              vname -> stringToType(inType.toString)
            }
            })


            val generateDef = CaseClassApplicativeBuilder.generateDef(of.map(t => t._1.encoded -> t._2.toString), typeToBuild.toString)


            if (of.toMap != argSpec.toMap) {
              c.abort(annottee.pos, "signature not matching to build " + typeToBuild.toString + ", use : \n " + generateDef.declaration + " = ???")
            }


            val result = c.parse(generateDef.all)

            c.Expr[Any](result)

          }
        }
      }
    }
  }
}


object ApplicativeKeyConstruct {
  def autoApplicativeImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = new ApplicativeKeyConstruct[c.type](c).autoApplicativeImpl(annottees: _*)

  def autoBuildResultImpl(c:Context)(annottees: c.Expr[Any]*): c.Expr[Any] = new ApplicativeKeyConstruct[c.type](c).autoBuildResultImpl(annottees: _*)
}
