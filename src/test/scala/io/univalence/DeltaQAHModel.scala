/** Dependencies :"org.typelevel" %% "shapeless-spire" % "0.4"
  *
  */
/** +-+-+-+-+-+-+-+
  * |D|e|l|t|a|Q|A|
  * +-+-+-+-+-+-+-+
  *
  * This job a compare two modeleH datasets by projecting them in a KPI 'vector space' (Rng).
  *
  *
  * The job first transform each visitor in a KPI Vector (lighter on shuffle) then join the result:
  *
  *       if the visitorId is only in the left dataset, then the KPI vector is added in
  *         DeltaDataset.left
  *
  *       if the visitorID is only in the right data, then the KPI vector is added in
  *         DeltaDataset.right
  *
  *       if the visitor is in both left and right, KPIs from both sides are compared,
  *       the difference (diff) and derived metrics (#zero, diff²) are added in
  *         DeltaDataset.both
  *
  *
  * KPI operations (Rng aka PseudoRing, ie 0, +, -, *) are based on spire and autoderived
  * by shapeless.contrib.spire.
  *
  */
package datalab.pj.validate {

  import datalab.pj.validate.hlight._
  import org.apache.spark.rdd.RDD

  import spire.algebra._

  import spire.implicits._

  //FOR AUTO-DERIVATION OF KPI CLASSES
  import shapeless.contrib.spire._

  case class DeltaJobParam(
      to:   String,
      from: String
  )

  object DeltaQA {

    def main(args: Array[String]) {

      val to:   String = ""
      val from: String = ""

      def read(s: String): RDD[(String, ByVisitor)] = ???

      //TODO : to print in JSON for clarity
      ////println(((compare(dest = read(to), orig = read(from)))

    }

    def compare(dest: RDD[(String, ByVisitor)], orig: RDD[(String, ByVisitor)]): DeltaDataset = {

      val zero = Monoid.additive[DeltaDataset].id

      val processJoin: ((Option[ByVisitor], Option[ByVisitor])) ⇒ DeltaDataset = {
        case (None,        Some(origH)) ⇒ zero.copy(orig = Traffic(1, origH))
        case (Some(destH), None) ⇒ zero.copy(dest = Traffic(1, destH))
        case (Some(destH), Some(origH)) ⇒
          zero.copy(both = DeltaDataset.change(destH, origH))
        case _ ⇒ zero // TO PLEASE THE COMPILER
      }

      dest
        .fullOuterJoin(orig)
        .values
        .map(processJoin)
        .reduce(_ + _)

    }
  }

  case class DeltaDataset(dest: Traffic, orig: Traffic, both: DeltaByVisitor)
  object DeltaDataset {

    def change(origKpi: ByVisitor, destKpi: ByVisitor): DeltaByVisitor = {

      val minusKpi = destKpi - origKpi

      DeltaByVisitor(
        nbVisitor = 1,
        nbZero    = if (minusKpi.isZero) 1 else 0,
        delta     = minusKpi,
        error     = minusKpi * minusKpi,
        orig      = origKpi,
        dest      = destKpi
      )
    }

  }

  case class Traffic(nbVisitor: Long, kpi: ByVisitor)

  case class DeltaByVisitor(
      nbVisitor: Long,
      nbZero:    Long,
      delta:     ByVisitor,
      error:     ByVisitor,
      orig:      ByVisitor,
      dest:      ByVisitor
  )

  /** ALL THE FOLLOWING CODE IS SPECIFIC TO THE MODELEH DELTA
    */
  //KPI DEFINITION
  case class ByVisitor(
      nbSearch:      Long,
      nbDisplayLR:   Long,
      nbFDMinLR:     Long,
      nbFDO:         Long,
      nbClicLR:      Long,
      nbClicFDMinLR: Long,
      nbClicFDO:     Long,
      // nbSearchWithBadLocality:         Long,
      // nbSearchDisplayEnrich:           Long,
      nbRechercheWithNonContinousBloc: Long
  ) {

    def isZero: Boolean = Monoid.additive[ByVisitor].id == this

  }

  //TRANSFORMATION FROM MODELEH-LIGHT TO KPI
  object ByVisitor {

    def fromVisiteur(visiteur: Visiteur): ByVisitor = {
      val lrs    = visiteur.recherches.flatMap(_.lrs)
      val fdSeos = visiteur.recherches.flatMap(_.fdSeo)

      ByVisitor(
        nbSearch      = visiteur.recherches.size,
        nbDisplayLR   = lrs.size,
        nbFDMinLR     = lrs.flatMap(_.fd).size,
        nbFDO         = fdSeos.size,
        nbClicLR      = lrs.flatMap(_.clics).size,
        nbClicFDMinLR = lrs.flatMap(_.fd).flatMap(_.clics).size,
        nbClicFDO     = fdSeos.flatMap(_.clics).size,
        nbRechercheWithNonContinousBloc = visiteur.recherches.count(r ⇒ {
          //TODO : faire la vérification seulement sur PagesJaunes
          // (les recherches sur Pages Blanches ne sont pas continues)
          val positions = r.lrs.flatMap(_.blocPosition).distinct
          if (positions.nonEmpty) {
            val max = positions.max
            val min = positions.min
            positions.size < max - min + 1
          } else false
        })
      )
    }
  }

}

package datalab.pj.validate.hlight {

  import org.apache.spark.sql.Row

  /** **
    *
    * FROM DATASCAN
    *
    * +-+-+-+-+-+-+-+ +-+-+-+-+-+
    * |M|O|D|E|L|E|H| |L|I|G|H|T|
    * +-+-+-+-+-+-+-+ +-+-+-+-+-+
    *
    */
  case class Visiteur(
      visitorId:  String,
      typeSource: String,
      sessions:   Seq[Session]
  ) {

    def recherches = sessions.flatMap(_.recherches)
  }

  case class Session(recherches: Seq[Recherche])

  case class Recherche(
      typeReponse:        String,
      typeAccesRecherche: String,
      //history:            Seq[PageRecherche],
      lrs:           Seq[Reponse],
      codesLieuxBag: Seq[CodeLieu],
      fdSeo:         Seq[FicheDetailleeOrpheline]
  )

  case class PageRecherche(
      bandeaux: Seq[BandeauReponse]
  )

  case class CodeLieu()

  case class FicheDetailleeOrpheline(clics: Seq[Clic])

  case class BandeauReponse(
      lrs: Seq[Reponse]
  )

  case class Reponse(
      blocNumeroClient: String,
      etablissements:   Seq[String],
      fd:               Seq[FicheDetaillee],
      clics:            Seq[Clic],
      blocPosition:     Option[Int]
  )

  case class FicheDetaillee(clics: Seq[Clic])

  case class Clic(coType: String, natureClic: Option[String])

  case class Annotation(
      level:    String,
      stage:    String,
      typeName: String,
      message:  String,
      path:     String
  )

  object ModeleHConverter {

    def visitorFromRow(r: Row): Visiteur = {
      Visiteur(
        visitorId  = r.getAs("visitorId"),
        typeSource = r.getAs("typeSource"),
        sessions   = r.getAs[Seq[Row]]("sessions").map(sessionFromRow).toVector
        //, annotations = r.getAs[Seq[Row]]("annotations").map(annotationFromRow).toList
      )
    }

    def annotationFromRow(r: Row): Annotation = {
      Annotation(
        level    = r.getAs[String]("level"),
        stage    = r.getAs[String]("stage"),
        typeName = r.getAs[String]("typeName"),
        message  = r.getAs[String]("message"),
        path     = r.getAs[String]("path")
      )
    }

    def sessionFromRow(r: Row): Session = {
      Session(
        recherches = r.getAs[Seq[Row]]("recherches").map(rechercheFromRow).toList
      )
    }

    def rechercheFromRow(r: Row): Recherche = {
      Recherche(
        typeReponse   = r.getAs[String]("typereponse"),
        codesLieuxBag = r.getAs[Seq[Row]]("codesLieuxBag").map(codeLieuFromRow),
        /*ACHTUNG*/
        lrs = {
          if (r.schema.fieldNames.contains("history"))
            r.getAs[Seq[Row]]("history")
              .map(pageRechercheFromRow)
              .flatMap(_.bandeaux)
              .flatMap(_.lrs)
          else
            r.getAs[Seq[Row]]("bandeaux").map(bandeauFromRow).flatMap(_.lrs)
        },
        typeAccesRecherche = r.getAs[String]("typeAccesRecherche"),
        fdSeo              = r.getAs[Seq[Row]]("fdSeo").map(ficheDetailleeOrphelineFromRow)
      )
    }

    def ficheDetailleeOrphelineFromRow(r: Row) =
      FicheDetailleeOrpheline(r.getAs[Seq[Row]]("clics").map(clicFromRow))

    def codeLieuFromRow(r: Row) = CodeLieu()

    def pageRechercheFromRow(r: Row): PageRecherche = {
      PageRecherche(
        bandeaux = r.getAs[Seq[Row]]("bandeaux").map(bandeauFromRow)
      )
    }

    def bandeauFromRow(r: Row): BandeauReponse = {
      BandeauReponse(
        lrs = r.getAs[Seq[Row]]("lrs").map(reponseFromRow)
      )
    }

    def reponseFromRow(r: Row): Reponse = {
      Reponse(
        blocNumeroClient = r.getAs[String]("blocNumeroClient"),
        etablissements   = r.getAs[Seq[String]]("etablissements"),
        fd               = r.getAs[Seq[Row]]("fd").map(ficheDetailleeFromRow),
        clics            = r.getAs[Seq[Row]]("clics").map(clicFromRow),
        blocPosition     = Option(r.getAs[Int]("blocPosition"))
      )
    }

    def clicFromRow(r: Row): Clic = {
      Clic(
        coType     = r.getAs[String]("coType"),
        natureClic = None
      )
    }

    def ficheDetailleeFromRow(r: Row): FicheDetaillee = {
      FicheDetaillee(
        clics = r.getAs[Seq[Row]]("clics").map(clicFromRow)
      )
    }
  }

}
