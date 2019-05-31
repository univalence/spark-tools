---
layout: home
position: 1
section: home
title: "Home"
---

# {{page.title}}

Spark-tools is a set of tools dedicated to Spark and aims to make the life of data engineers easier.

## Getting started with Spark Test

We recommend you to start using spark-tools with spark-test which is the most accessible tool.

Here is an example from A to Z using spark-test :

Include [Spark-test](./spark-test) to your project implementing these following lines inside your build sbt :

```scala
resolvers += "spark-test" at "http://dl.bintray.com/univalence/univalence-jvm"

libraryDependencies += "io.univalence" %% "spark-test" % "0.2+245-09a064d9" % Test
```

For example, Spark-test provides a assertEquals function which compare two RDDs, Datasets or Dataframes returning an AssertionError if they are different.


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

There are many other functionalities (To learn more about Spark-Test, see the [Spark-test documentation](./spark-test)).

## The tools

* [Centrifuge](https://github.com/univalence/spark-tools/tree/master/centrifuge), a couple of techniques for data quality on Spark
* [Fenek](https://github.com/univalence/spark-tools/tree/master/fenek), a DSL for *semi-typed* transformation in Scala for Spark
* [Plumbus](https://github.com/univalence/spark-tools/tree/master/plumbus), light misc things for Spark
* [Spark-ZIO](https://github.com/univalence/spark-tools/tree/master/spark-zio), Spark in ZIO environment
