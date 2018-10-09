scalacOptions ++= Seq("-Yrangepos", "-unchecked", "-deprecation")

version      := "0.1"
name         := "centrifuge"
organization := "io.univalence"
scalaVersion := "2.11.12"

resolvers ++= Seq(
  "sonatype-oss"      at "http://oss.sonatype.org/content/repositories/snapshots",
  "OSS"               at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Conjars"           at "http://conjars.org/repo",
  "Clojars"           at "http://clojars.org/repo",
  "m2-repo-github"    at "https://github.com/ahoy-jon/m2-repo/raw/master"
)

libraryDependencies ++= Seq(
  "com.chuusai"    %% "shapeless"        % "2.2.5",
  "org.scalaz"     %% "scalaz-core"      % "7.1.4",
  "org.spire-math" %% "spire"            % "0.13.0",
  "org.typelevel"  %% "shapeless-spire"  % "0.6.1",
  "org.typelevel"  %% "shapeless-scalaz" % "0.4",
  "org.scala-lang" % "scala-reflect"     % scalaVersion.value,
  "io.monix"       %% "monix"            % "2.3.3",
  "org.typelevel"  %% "cats-core"        % "1.0.0",
  "org.typelevel"  %% "cats-laws"        % "1.0.0"
)

def spark(names: String*) = names.map(name => "org.apache.spark" %% s"spark-$name" % "2.1.1" % Provided)

libraryDependencies ++= spark("core", "sql", "mllib")

libraryDependencies ++= Seq(
  "org.scalatest"  %% "scalatest"  % "3.0.3"  % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
)

scalafmtOnCompile in ThisBuild     := true
scalafmtTestOnCompile in ThisBuild := true

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

////scalafmt without the plugin
//lazy val format = TaskKey[Unit]("scalafmt", "FMT Files")
//
//format := {
//  import sys.process.Process
//  Process("./scalafmt --non-interactive --git true", baseDirectory.value).!
//}
