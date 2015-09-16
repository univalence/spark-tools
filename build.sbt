
name := "autobuild"

organization := "io.univalence"

scalaVersion := "2.11.7"

resolvers += "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots"

resolvers += "OSS" at "http://oss.sonatype.org/content/repositories/releases"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Conjars" at "http://conjars.org/repo"

resolvers += "Clojars" at "http://clojars.org/repo"

libraryDependencies ++= Seq("com.chuusai" % "shapeless_2.11" % "2.2.5")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

publishTo := Some(Resolver.file("file",  new File( "/Users/jon/Project/m2-repo")) )
