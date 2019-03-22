import ScalaSettings._

organization         := "io.univalence"
organizationName     := "Univalence"
organizationHomepage := Some(url("https://univalence.io/"))

name := "spark-tools"

version := "0.2-SNAPSHOT"

val defaultConfiguration =
  Def.settings(
    crossScalaVersions := List("2.11.12", "2.12.8"),
    //By default projects in spark-tool work with 2.11 and are ready for 2.12
    //Spark projects are locked in 2.11 at the moment
    scalaVersion  := crossScalaVersions.value.head,
    scalacOptions := stdOptions ++ extraOptions(scalaVersion.value)
  )

licenses := Seq("The Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/univalence/spark-tools"),
    "scm:git:https://github.com/univalence/spark-tools.git",
    "scm:git:git@github.com:univalence/spark-tools.git"
  )
)

publishTo := sonatypePublishTo.value
useGpg    := true

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

lazy val centrifuge = project
  .settings(defaultConfiguration: _*)
  .settings(
    description := "Centrifuge is for data quality",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/centrifuge")),
    startYear   := Some(2017),
    libraryDependencies ++= Seq(
      "com.chuusai"   %% "shapeless"       % "2.3.3",
      "org.scalaz"    %% "scalaz-core"     % "7.2.27",
      "org.typelevel" %% "shapeless-spire" % "0.6.1",
      //"org.typelevel" %% "shapeless-scalaz" % "0.6.1",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "io.monix"       %% "monix"        % "2.3.3",
      "org.typelevel"  %% "cats-core"    % "1.0.0",
      "org.typelevel"  %% "cats-laws"    % "1.0.0"
    ),
    useSpark(sparkVersion = "2.1.1")(modules = "core", "sql", "mllib"),
    addTestLibs,
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
  )

lazy val fenek = project
  .settings(defaultConfiguration)
  .settings(
    libraryDependencies ++= Seq("joda-time" % "joda-time" % "2.10", "org.json4s" %% "json4s-native" % "3.2.11"),
    useSpark("2.0.0")("sql"),
    addTestLibs
  )

lazy val plumbus =
  project
    .settings(defaultConfiguration)
    .settings(
      name        := "plumbus",
      description := "Collection of tools for Scala Spark",
      startYear   := Some(2019),
      licenses    := Seq("The Apache License, Version 2.0" → url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    )
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-sql" % "2.4.0",
        "com.propensive"   %% "magnolia"  % "0.10.0"
      ),
      libraryDependencies ++= Seq(
        "org.scalatest"  %% "scalatest"  % "3.0.5",
        "org.scalacheck" %% "scalacheck" % "1.13.4"
      ).map(_ % Test)
    )
    .settings(
      scalafmtOnCompile := true,
      publishTo         := sonatypePublishTo.value,
      useGpg            := true
    )

lazy val typedpath = project
  .settings(defaultConfiguration)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "eu.timepit"     %% "refined"      % "0.9.4"
    ),
    addTestLibs
  )

def addTestLibs: SettingsDefinition =
  libraryDependencies ++= Seq(
    "org.scalatest"  %% "scalatest"  % "3.0.3"  % Test,
    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test
  )

def useSpark(sparkVersion: String)(modules: String*): SettingsDefinition =
  libraryDependencies ++= {
    val minVersion: String = CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 11)) => "2.0.0"
      case Some((2, 12)) => "2.4.0"
      case x             => throw new Exception(s"unsupported scala version $x for Spark")
    }
    val bumpedVersion = Seq(sparkVersion, minVersion).max

    modules.map(name => "org.apache.spark" %% s"spark-$name" % bumpedVersion % Provided)
  }

scalafmtOnCompile := false

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")

addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

resolvers ++= Seq(
  "sonatype-oss"      at "http://oss.sonatype.org/content/repositories/snapshots",
  "OSS"               at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Conjars"           at "http://conjars.org/repo",
  "Clojars"           at "http://clojars.org/repo",
  "m2-repo-github"    at "https://github.com/ahoy-jon/m2-repo/raw/master"
)
