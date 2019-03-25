import ScalaSettings._
import Dependencies._

name := "spark-tools"

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

lazy val defaultConfiguration =
  Def.settings(
    version            := "0.2",
    crossScalaVersions := List(libVersion.scala2_11, libVersion.scala2_12),
    //By default projects in spark-tool work with 2.11 and are ready for 2.12
    //Spark projects are locked in 2.11 at the moment
    scalaVersion      := crossScalaVersions.value.head,
    scalacOptions     := stdOptions ++ extraOptions(scalaVersion.value),
    useGpg            := true,
    scalafmtOnCompile := false,
    publishTo         := sonatypePublishTo.value
  )

// ====

lazy val centrifuge = project
  .settings(projectDescription, defaultConfiguration)
  .settings(
    name        := "Centrifuge",
    description := "Centrifuge is for data quality",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/centrifuge")),
    startYear   := Some(2017)
  )
  .settings(
    libraryDependencies ++= Seq(
      "com.chuusai"   %% "shapeless"       % libVersion.shapeless,
      "org.scalaz"    %% "scalaz-core"     % libVersion.scalaz,
      "org.typelevel" %% "shapeless-spire" % "0.6.1",
      //"org.typelevel" %% "shapeless-scalaz" % "0.6.1",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "io.monix"       %% "monix"        % libVersion.monix,
      "org.typelevel"  %% "cats-core"    % libVersion.cats,
      "org.typelevel"  %% "cats-laws"    % libVersion.cats
    ),
    useSpark(sparkVersion = "2.1.1")(modules = "core", "sql", "mllib"),
    addTestLibs,
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
  )

lazy val fenek = project
  .settings(projectDescription, defaultConfiguration)
  .settings(
    name        := "Fenek",
    description := "Fenek",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/fenek")),
    startYear   := Some(2018),
  )
  .settings(
    libraryDependencies ++= Seq("joda-time"  % "joda-time"      % libVersion.jodaTime,
                                "org.json4s" %% "json4s-native" % libVersion.json4s),
    useSpark(libVersion.sparkScala211)("sql"),
    addTestLibs
  )

lazy val plumbus =
  project
    .settings(projectDescription, defaultConfiguration)
    .settings(
      name        := "Plumbus",
      description := "Collection of tools for Scala Spark",
      homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/plumbus")),
      startYear   := Some(2019)
    )
    .settings(
      useSpark(libVersion.sparkScala212)("sql"),
      libraryDependencies ++= Seq("com.propensive" %% "magnolia" % libVersion.magnolia),
      addTestLibs
    )

lazy val typedpath = project
  .settings(projectDescription, defaultConfiguration)
  .settings(
    name        := "Typedpath",
    description := "Typedpath",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/typedpath")),
    startYear   := Some(2019)
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    addTestLibs
  )

// ====

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
