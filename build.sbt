
scalacOptions ++= Seq("-Yrangepos", "-unchecked", "-deprecation")

version := "0.2-SNAPSHOT"

name := "autobuild"

organization := "io.univalence"

scalaVersion := "2.11.7"

resolvers += "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots"

resolvers += "OSS" at "http://oss.sonatype.org/content/repositories/releases"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Conjars" at "http://conjars.org/repo"

resolvers += "Clojars" at "http://clojars.org/repo"

libraryDependencies += "com.chuusai" % "shapeless_2.11" % "2.2.5"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.4"

libraryDependencies += "org.typelevel" %% "shapeless-scalaz" % "0.4"

libraryDependencies += "io.univalence" %% "excelsius" % "0.1-SNAPSHOT"

//2.1.0-SNAPSHOT
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

publishTo := Some(Resolver.file("file",  new File( "/Users/jon/Project/m2-repo")) )

resolvers in ThisBuild  ++= Seq("m2-repo-github" at "https://github.com/ahoy-jon/m2-repo/raw/master")