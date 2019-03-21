organization         := "io.univalence"
organizationName     := "Univalence"
organizationHomepage := Some(url("https://univalence.io/"))

name := "spark-tools"

version := "0.2-SNAPSHOT"

val defaultConfiguration = Seq(
  crossScalaVersions := List("2.11.12", "2.12.8"),
  //By default projects in spark-tool work with 2.11 and are ready for 2.12
  //Spark projects are locked in 2.11 at the moment
  scalaVersion := crossScalaVersions.value.head
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
  )
)

lazy val centrifuge = project
  .settings(defaultConfiguration: _*)
  .settings(
    description := "Centrifuge is for data quality",
    homepage    := Some(url("https://github.com/univalence/spark-tools/tree/master/centrifuge")),
    startYear   := Some(2017),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless"   % "2.3.3",
      "org.scalaz"  %% "scalaz-core" % "7.2.27",
      //"org.typelevel" %% "spire" % "0.15.0",
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
  .settings(defaultConfiguration: _*)
  .settings(
    libraryDependencies ++= Seq("joda-time" % "joda-time" % "2.10", "org.json4s" %% "json4s-native" % "3.2.11"),
    useSpark("2.0.0")("sql"),
    addTestLibs
  )

lazy val plumbus   = project.settings(defaultConfiguration: _*)
lazy val typedpath = project.settings(defaultConfiguration: _*)

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

scalacOptions := Opts.compile.encoding("utf-8")

scalacOptions ++= Seq("-Yrangepos", "-unchecked", "-deprecation")

scalacOptions ++= Seq(
  Opts.compile.deprecation, // Emit warning and location for usages of deprecated APIs.
  Opts.compile.explaintypes, // Explain type errors in more detail.
  Opts.compile.unchecked, // Enable additional warnings where generated code depends on assumptions.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views           "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  //            "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xfuture", // Turn on future language features.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match", // Pattern match may not be typesafe.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ypatmat-exhaust-depth",
  "off",
  "-Ywarn-numeric-widen" // Warn when numerics are widened.
)

resolvers ++= Seq(
  "sonatype-oss"      at "http://oss.sonatype.org/content/repositories/snapshots",
  "OSS"               at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Conjars"           at "http://conjars.org/repo",
  "Clojars"           at "http://clojars.org/repo",
  "m2-repo-github"    at "https://github.com/ahoy-jon/m2-repo/raw/master"
)
