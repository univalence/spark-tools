package fix

import scalafix.v1._

import scala.collection.immutable
import scala.meta._

object SelectField {

  def unapply(tree: Tree): Option[Vector[String]] =
    tree match {
      case Term.Select(Term.Name(">"), Term.Name(arg))                                 => Some(Vector(arg))
      case Term.Select(Term.Select(SelectField(args), Term.Name(">")), Term.Name(arg)) => Some(args :+ arg)
      case _                                                                           => None
    }
}

object PathFrom {

  def unapply(tree: Tree): Option[(String, Vector[String])] =
    tree match {
      case Term.Select(Term.Select(Term.Name(pos), Term.Name(">")), Term.Name(arg)) =>
        Some((pos, Vector(arg)))
      case Term.Select(Term.Select(PathFrom(pos, args), Term.Name(">")), Term.Name(arg)) =>
        Some((pos, args :+ arg))
      case _ => None
    }
}

class Fenek2to3Path extends SemanticRule("Fenek2to3Path") {
  override def fix(implicit doc: SemanticDocument): Patch = {

    val pathRewrite: Seq[Patch] = doc.tree
      .collect({
        case s @ SelectField(args) =>
          (s.pos, Patch.replaceTree(s, args.mkString("path\"", ".", "\"")))

        case p @ PathFrom(root, args) =>
          (p.pos, Patch.replaceTree(p, args.mkString("path\"$" + root + ".", ".", "\"")))
      })
      .groupBy(_._1.start)
      .map(_._2.maxBy(_._1.text.length)._2)
      .toSeq

    pathRewrite.fold(Patch.empty)(_ + _)
  }

}

class Fenek2to3Import extends SemanticRule("Fenek2to3Import") {
  override def fix(implicit doc: SemanticDocument): Patch = {
    val patches = doc.tree.collect({
      case i @ Importer(ref, xs) if ref.toString.contains("Fnk") || ref.toString.contains("TypedExpr") =>
        xs.map(Patch.removeImportee)

    })

    if (patches.nonEmpty) {
      patches.flatten.reduce(_ + _) +
        Patch.addGlobalImport(importer"io.univalence.fenek.Expr._") +
        Patch.addGlobalImport(importer"io.univalence.typedpath.Path._") +
        Patch.addGlobalImport(importer"io.univalence.typedpath.Path") +
        Patch.addGlobalImport(importer"io.univalence.fenek.Expr")
    } else Patch.empty
  }
}

class Fenek2to3Rest extends SemanticRule("Fenek2to3Rest") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    //println("Tree.structureLabeled: " + doc.tree.structureLabeled)

    val patches: Seq[Seq[Patch]] = doc.tree.collect({

      case t @ Type.Name("Expr") =>
        Seq(Patch.replaceTree(t, "Expr[Any]"))

      case t @ Type.Apply(Type.Name("TypedExpr"), x :: Nil) =>
        Seq(Patch.replaceTree(t, s"Expr[$x]"))


      case  t @ Type.Select(Term.Name("Fnk"), _) =>
        Seq(Patch.removeTokens(t.tokens.take(2)))

      case s @ Term.Apply(Term.Name("struct"), args) if args.forall(x => x.isInstanceOf[Term.Assign]) =>
        val assigns = args.asInstanceOf[Seq[Term.Assign]]
        assigns.map({
          case t @ Term.Assign(n @ Term.Name(_), rhs) =>
            val equalToken = t.tokens.tokens.drop(n.tokens.end).find(_.text == "=").get
            Patch.addLeft(n, "\"") +
              Patch.addRight(n, "\"") +
              Patch.removeToken(equalToken) +
              Patch.addLeft(rhs, "<<- ")
        })

      case Term.ApplyInfix(_, p @ Term.Name("|"), _, _) =>
        Seq(Patch.replaceTree(p, ","))
    })

    //println("Tree.syntax: " + doc.tree.syntax)
    //println("Tree.structure: " + doc.tree.structure)


    patches.flatten.fold(Patch.empty)(_ + _)
  }

}
