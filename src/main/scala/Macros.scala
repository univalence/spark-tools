package io.univalence.autobuild

import java.nio.file.{Paths, Files}

import net.ironforged.scaladiff.{Operation, OperationType, Diff}
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



  def generateDef3(fieldNames1:List[(String,String)], name:String, a:String): Generated = {
    val signature:String = "def build(" + fieldNames1.map({case (n,t) => s"$n : $a[$t]"}).mkString(",\n") + s"):$a[" + name + "]"

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



  def generateDef(fieldnames1: List[(String, String)], name: String, containerName:String): Generated = {
    val signature: String = "def build(" + fieldnames1.map({ case (n, t) => s"$n : $containerName[$t]" }).mkString(",\n") + s"):$containerName[" + name + "]"


    val zip = "def zip[A,B](x:A)(y:B):(A,B)=(x,y)"
    val zipC = s"def zipC[A,B](x:$containerName[A])(y:$containerName[B]): $containerName[(A,B)] = y.app(x.map(zip))"


    val impl: String = fieldnames1 match {

      case (x :: xs) => {

        val prefix = xs.foldLeft(x._1)((acc, next) => {
          s"zipC(${next._1})($acc)"
        })

        def access(n: String, i: Int): String = n + " = t" + ("._2" * i) + "._1"

        val fimpl = name + "(" + (x._1 + " = t" + ("._2" * xs.length)) + "," + xs.reverse.map(_._1).zipWithIndex.map(t => access(t._1, t._2)).mkString(", ") + ")"


        prefix + s".map(t => $fimpl )"

      }

      case Nil => ???


    }

    Generated(signature, List(zip, zipC, impl).mkString("{","\n","}"))


  }
}


@compileTimeOnly("enable macro paradise to expand macro annotations")
class autoBuildApp extends StaticAnnotation {
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


class ApplicativeKeyConstruct[C <: whitebox.Context](val c: C) extends CaseClassMacros {


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


            val generateDef = CaseClassApplicativeBuilder.generateDef2(of.map(t => t._1.encoded ->
              CleanTypeName.clean(t._2.toString)), CleanTypeName.clean(typeToBuild.toString))


            if (of.toMap != argSpec.toMap) {





              val file = c.enclosingPosition.source.file


              val source: String = scala.io.Source.fromFile(file.file).mkString
              val start: Int = c.enclosingPosition.start - 1
              val create: Diff = Diff.create("@autoBuildResult\n" + annottee.toString(),
                source.drop(start))




              val last: String = create.diffs.reverse.takeWhile(_.op == OperationType.Insert).map(_.text).mkString.reverse

              val toDrop: String = source.drop(start).reverse.drop(last.length).reverse

              val nbToClose = Math.max(toDrop.count(_ == '{') - toDrop.count(_ == '}') -2,0)

              val lastStartIndex =  last.indexOf('\n', (0 until nbToClose).foldLeft(-1)((i,_) => last.indexOf('}',i + 1)))

              val content: String = source.take(start) +"\n@autoBuildResult \n" +  generateDef.all+  last.take(lastStartIndex) + last.drop(lastStartIndex-1)


              Files.write(file.file.toPath, content.getBytes)



              c.abort(annottee.pos, "signature not matching to build " + typeToBuild.toString + ", source code updated")
            }


            val result = c.parse(generateDef.all)

            c.Expr[Any](result)

          }
        }
      }
    }
  }


  def autoBuildapp(annottees: c.Expr[Any]*): c.Expr[Any] = {
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
                case tq"$b[$paramInType]" => paramInType
              }
              vname -> stringToType(inType.toString)
            }
            })


            val generateDef = CaseClassApplicativeBuilder.generateDef(of.map(t => t._1.encoded ->
              CleanTypeName.clean(t._2.toString)), CleanTypeName.clean(typeToBuild.toString), a.toString)


            if (of.toMap != argSpec.toMap) {





              val file = c.enclosingPosition.source.file


              val source: String = scala.io.Source.fromFile(file.file).mkString
              val start: Int = c.enclosingPosition.start - 1
              val create: Diff = Diff.create("@autoBuildApp\n" + annottee.toString(),
                source.drop(start))




              val last: String = create.diffs.reverse.takeWhile(_.op == OperationType.Insert).map(_.text).mkString.reverse

              val toDrop: String = source.drop(start).reverse.drop(last.length).reverse

              val nbToClose = Math.max(toDrop.count(_ == '{') - toDrop.count(_ == '}'),0)

              val lastStartIndex =  last.indexOf('\n', (0 until nbToClose).foldLeft(-1)((i,_) => last.indexOf('}',i + 1)))

              val content: String = source.take(start) +"\n@autoBuildApp \n" +  generateDef.all+  last.take(lastStartIndex) + last.drop(lastStartIndex-1)


              Files.write(file.file.toPath, content.getBytes)



              c.abort(annottee.pos, "signature not matching to build " + typeToBuild.toString + ", source code updated")
            }


            val result = c.parse(generateDef.all)

            //c.Expr[Any](result)
            annottees.head

          }
        }
      }
    }
  }

  /*def autoApplicativeImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
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


            val generateDef = CaseClassApplicativeBuilder.generateDef(of.map(t => t._1.encoded -> t._2.toString), typeToBuild.toString, a.toString)


            if (of.toMap != argSpec.toMap) {
              c.abort(annottee.pos, "signature not matching to build " + typeToBuild.toString + ", use : \n " + generateDef.declaration + " = ???")
            }


            val result = c.parse(generateDef.all)

            c.Expr[Any](result)

          }
        }
      }
    }
  }*/
}


object ApplicativeKeyConstruct {
  def autoApplicativeImpl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = new ApplicativeKeyConstruct[c.type](c).autoBuildapp(annottees: _*)

  def autoBuildResultImpl(c:whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = new ApplicativeKeyConstruct[c.type](c).autoBuildResultImpl(annottees: _*)
}
