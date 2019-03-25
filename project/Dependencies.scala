import sbt._
import sbt.Keys._

object Dependencies {

  val libVersion =
    new {
      val cats          = "1.0.0"
      val jodaTime      = "2.10"
      val json4s        = "3.2.11"
      val magnolia      = "0.10.0"
      val monix         = "2.3.3"
      val scala2_12     = "2.12.8"
      val scala2_11     = "2.11.12"
      val scalacheck    = "1.13.5"
      val scalatest     = "3.0.5"
      val scalaz        = "7.2.27"
      val shapeless     = "2.3.3"
      val sparkScala211 = "2.0.0"
      val sparkScala212 = "2.4.0"
    }

  def addTestLibs: SettingsDefinition =
    libraryDependencies ++= Seq(
      "org.scalatest"  %% "scalatest"  % libVersion.scalatest  % Test,
      "org.scalacheck" %% "scalacheck" % libVersion.scalacheck % Test
    )

  def useSpark(sparkVersion: String)(modules: String*): SettingsDefinition =
    libraryDependencies ++= {
      val minVersion: String =
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, 11)) => libVersion.sparkScala211
          case Some((2, 12)) => libVersion.sparkScala212
          case x             => throw new Exception(s"unsupported scala version $x for Spark")
        }
      val bumpedVersion = Seq(sparkVersion, minVersion).max

      modules.map(name => "org.apache.spark" %% s"spark-$name" % bumpedVersion % Provided)
    }

}
