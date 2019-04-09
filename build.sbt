import ScalaSettings._
import java.util.UUID

lazy val projectDescription =
  Def.settings(
    organization         := "io.univalence",
    organizationName     := "Univalence",
    organizationHomepage := Some(url("https://univalence.io/")),
    licenses             := Seq("The Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/univalence/spark-tools"),
        "scm:git:https://github.com/univalence/spark-tools.git",
        "scm:git:git@github.com:univalence/spark-tools.git"
      )
    ),
    developers := List(
      Developer(
        id    = "jwinandy",
        name  = "Jonathan Winandy",
        email = "jonathan@univalence.io",
        url   = url("https://github.com/ahoy-jon")
      ),
      Developer(
        id    = "phong",
        name  = "Philippe Hong",
        email = "philippe@univalence.io",
        url   = url("https://github.com/hwki77")
      ),
      Developer(
        id    = "fsarradin",
        name  = "François Sarradin",
        email = "francois@univalence.io",
        url   = url("https://github.com/fsarradin")
      ),
      Developer(
        id    = "bernit77",
        name  = "Bernarith Men",
        email = "bernarith@univalence.io",
        url   = url("https://github.com/bernit77")
      ),
      Developer(
        id    = "HarrisonCheng",
        name  = "Harrison Cheng",
        email = "harrison@univalence.io",
        url   = url("https://github.com/HarrisonCheng")
      )
    )
  )

// ====

lazy val sparkTools = (project in file("."))
  .dependsOn(centrifuge, fenek, typedpath, plumbus, sparkZio)
  .settings(
    name        := "spark-tools",
    description := "Spark tools",
    startYear   := Some(2019),
    homepage    := Some(url("https://github.com/univalence/spark-tools"))
  )
  .settings(projectDescription, defaultConfiguration)

lazy val sparkZio = (project in file("spark-zio"))
  .settings(
    name        := "spark-zio",
    description := "Spark Zio is to use Zio with Spark",
    startYear   := Some(2019),
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/spark-zio"))
  )
  .settings(projectDescription, defaultConfiguration, deliveryConfiguration)
  .settings(
    useSpark(sparkVersion = "2.1.1")(modules = "sql"),
    libraryDependencies += "org.scalaz" %% "scalaz-zio" % "0.19"
  )

lazy val centrifuge = project
  .settings(projectDescription, defaultConfiguration, deliveryConfiguration)
  .settings(
    description        := "Centrifuge is for data quality",
    homepage           := Some(url("https://github.com/univalence/spark-tools/tree/master/centrifuge")),
    startYear          := Some(2017),
    crossScalaVersions := List(libVersion.scala2_11)
  )
  .settings(
    libraryDependencies ++= Seq(
      "com.chuusai"    %% "shapeless"       % libVersion.shapeless,
      "org.scalaz"     %% "scalaz-core"     % libVersion.scalaz,
      "org.typelevel"  %% "shapeless-spire" % "0.6.1",
      "org.scala-lang" % "scala-reflect"    % scalaVersion.value,
      "io.monix"       %% "monix"           % libVersion.monix,
      "org.typelevel"  %% "cats-core"       % libVersion.cats,
      "org.typelevel"  %% "cats-laws"       % libVersion.cats
    ),
    useSpark(sparkVersion = "2.1.1")(modules = "core", "sql", "mllib"),
    addTestLibs,
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
  )

lazy val fenek = project
  .dependsOn(typedpath)
  .settings(projectDescription, defaultConfiguration, deliveryConfiguration)
  .settings(
    description := "Fenek is for better mapping",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/fenek")),
    startYear   := Some(2018),
    libraryDependencies ++= Seq("joda-time"  % "joda-time"      % libVersion.jodaTime,
                                "org.json4s" %% "json4s-native" % libVersion.json4s),
    useSpark(libVersion.sparkScala211)("sql"),
    addTestLibs
  )

lazy val plumbus =
  project
    .settings(projectDescription, defaultConfiguration, deliveryConfiguration)
    .settings(
      description := "Collection of tools for Scala Spark",
      homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/plumbus")),
      startYear   := Some(2019)
    )
    .settings(
      useSpark(libVersion.sparkScala212)("sql"),
      libraryDependencies ++= Seq(
        "com.propensive" %% "magnolia" % libVersion.magnolia,
        "MrPowers" % "spark-fast-tests" % "2.3.1_0.15.0" % Test
      ),
      addTestLibs
    )
    .settings(resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven")

lazy val typedpath = project
  .settings(projectDescription, defaultConfiguration, deliveryConfiguration)
  .settings(
    description := "Typedpath are for refined path",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/typedpath")),
    startYear   := Some(2019)
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    addTestLibs
  )

lazy val site = project
  .enablePlugins(MicrositesPlugin)
//  .dependsOn(centrifuge, plumbus, typedpath, fenek)
  .settings(
    name := "spark-tools-site"
  )
  .settings(
    micrositeName                 := "Spark tools",
    micrositeDescription          := "Tools for Spark to simplify the life of data engineers",
    micrositeAuthor               := "Spark tools contributors",
    micrositeOrganizationHomepage := "https://github.com/univalence/spark-tools",
    micrositeGitHostingUrl        := "https://github.com/univalence/spark-tools",
    micrositeFooterText := Some(
      """
        |<p>&copy; 2017-2019 <a href="https://github.com/univalence/spark-tools">Spark tools Maintainers</a></p>
        |""".stripMargin
    )
  )

// ====

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

lazy val defaultConfiguration =
  Def.settings(
    // By default projects in spark-tool work with 2.11 and are ready for 2.12
    // Spark projects are locked in 2.11 at the moment
    crossScalaVersions := List(libVersion.scala2_11, libVersion.scala2_12),
    scalaVersion       := libVersion.scala2_11,
    scalacOptions      := stdOptions ++ extraOptions(scalaVersion.value),
    scalafmtOnCompile  := false,
    parallelExecution  := false,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
  )

lazy val deliveryConfiguration =
  Def.settings(
    publishTo                  := sonatypePublishTo.value,
    releaseEarlyWith in Global := SonatypePublisher,
    isSnapshot                 := false,
    // XXX: set the value below to true if you really wish to deliver from your machine
    releaseEarlyEnableLocalReleases := false
  )

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

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")

addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Conjars"           at "http://conjars.org/repo",
  "Clojars"           at "http://clojars.org/repo",
  "m2-repo-github"    at "https://github.com/ahoy-jon/m2-repo/raw/master"
)
