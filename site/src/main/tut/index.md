---
layout: home
position: 1
section: home
title: "Home"
---

# {{page.title}}

Spark-Tools is a set of tools dedicated to providing more clarity for data engineers when working on Spark jobs.

Our tools were created based on the usage of meta-data, which allows for greater visibility on code structures. This in turn provides more context when something breaks down and needs fixing.

Imagine you're stumbling through your room in the dark and walk straight onto a piece of Lego; you curse, turn on the light and see that piece of Lego. Spark-Tools is that light: it won't stop the pain, but at least you know what hurt you.

## Getting started with Spark-Test

Spark-Test is the tool that will improve your test reports.

We recommend you to start using Spark-Tools with Spark-Test, because it's the most accessible tool. Here's an example of how to use Spark-Test from A to Z.

Include [Spark-Test](./spark-test) in your project by implementing the following lines inside your build sbt:

```scala
resolvers += "spark-test" at "http://dl.bintray.com/univalence/univalence-jvm"

libraryDependencies += "io.univalence" %% "spark-test" % "0.2+245-09a064d9" % Test
```

Spark-Test provides an `assertEquals` function which compares two RDDs, Datasets or Dataframes. It returns an `SparkTestError`
if they are different.

```scala
import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuiteLike

class MyTestClass extends FunSuiteLike with SparkTest {
    test("some test"){
        case class A(a:Int)
        
        val df = dataframe("{a:1, b:true}", "{a:2, b:false}")
        val ds = dataset(A(1), A(3))
        
        df.assertEquals(ds)
    }
}
```

```scala
java.lang.AssertionError: The data set content is different :
in field a, 2 is not equals to expected value 3 
dataframe("{ a: 2 , b: false}")
dataframe("{ a: 3 }")
```

There are many other features! To learn more about Spark-Test, see the [Spark-Test documentation](./spark-test).

## The tools

Each tools are open source and available on Github

* [Spark-test](https://github.com/univalence/spark-tools/tree/master/spark-test), testing tools for Spark
* [Parka](https://github.com/univalence/spark-tools/tree/master/parka), a tool that applies deltaQA for Datasets
* [Plumbus](https://github.com/univalence/spark-tools/tree/master/plumbus), light misc things for Spark
* [Fenek](https://github.com/univalence/spark-tools/tree/master/fenek), a DSL for *semi-typed* transformation in Scala for Spark
* [Spark-ZIO](https://github.com/univalence/spark-tools/tree/master/spark-zio), Spark in ZIO environment
