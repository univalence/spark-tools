lazy val plumbus =
  (project in file("."))
    .settings(metaSettings, scalaSettings)
    .settings(
      name := "spark-plumbus",
      version := "0.0.1-SNAPSHOT",
      description := "Collection of tools for Scala Spark",
      startYear := Some(2019),
      licenses := Seq("The Apache License, Version 2.0" → url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
      homepage := Some(url("https://github.com/UNIVALENCE/spark-plumbus"))
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
      publishTo := sonatypePublishTo.value,
      useGpg := true
    )

lazy val scalaSettings =
  Def.settings(
    scalaVersion := "2.11.12",
    scalacOptions :=
      Opts.compile.encoding("utf-8") // Specify character encoding used by source files (linked to the previous item).
        ++ Seq(
          Opts.compile.deprecation, // Emit warning and location for usages of deprecated APIs.
          Opts.compile.explaintypes, // Explain type errors in more detail.
          Opts.compile.unchecked, // Enable additional warnings where generated code depends on assumptions.
          "-feature", // Emit warning and location for usages of features that should be imported explicitly.
          "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
          "-language:experimental.macros", // Allow macro definition (besides implementation and application)
          "-language:higherKinds", // Allow higher-kinded types
          "-language:implicitConversions", // Allow definition of implicit functions called views
          "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
          "-Xfatal-warnings", // Fail the compilation if there are any warnings.
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
          "-Ywarn-numeric-widen" // Warn when numerics are widened.
        )
  )

lazy val metaSettings =
  Def.settings(
    organization := "io.univalence",
    organizationName := "Univalence",
    organizationHomepage := Some(url("http://univalence.io/")),
    developers := List(
      Developer(
        id = "jwinandy",
        name = "Jonathan Winandy",
        email = "jonathan@univalence.io",
        url = url("https://github.com/ahoy-jon")
      ),
      Developer(
        id = "fsarradin",
        name = "François Sarradin",
        email = "francois@univalence.io",
        url = url("https://github.com/fsarradin")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/UNIVALENCE/spark-plumbus"),
        "scm:git:https://github.com/UNIVALENCE/spark-plumbus.git",
        "scm:git:git@github.com:UNIVALENCE/spark-plumbus.git"
      ))
  )
