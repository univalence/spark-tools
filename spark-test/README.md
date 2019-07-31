Spark-Test
======================

Spark-Test is a library to help developers test their Spark applications.

Our goal is to provide you with tools that can help you test your code fast and in an easy way, precisely pointing out why something bad occurred with your Spark collections.

You just have to extend SparkTest to have all those tools!

## Installation

### Stable version

Version for scala 2.11.X :

```scala
libraryDependencies += "io.univalence" % "spark-test_2.11" % "0.3" % Test
```

Version for scala 2.12.X :

```scala
libraryDependencies += "io.univalence" % "spark-test_2.12" % "0.3" % Test
```

### Latest version

If you want to get the very last version of this library you can still download it using bintray here : https://bintray.com/univalence/univalence-jvm/spark-test

Here is an example using ```version 0.3+79-4936e981``` that work with ```scala 2.11.X```:

```scala
resolvers += "spark-test" at "http://dl.bintray.com/univalence/univalence-jvm"
libraryDependencies += "io.univalence" %% "spark-test" % "0.3+79-4936e981" % Test
```

## Documentation  
You can find Spark-Test's latest documentation [here](https://www.javadoc.io/doc/io.univalence/spark-test_2.11).