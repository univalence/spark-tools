


organization := "io.univalence"

name := "typedpath"

crossScalaVersions := List("2.11.12", "2.12.8")

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "eu.timepit" %% "refined" % "0.9.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
