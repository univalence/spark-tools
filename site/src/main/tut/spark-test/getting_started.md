---
layout: docs
section: spark-test
title: "Getting started"
---

# {{page.title}}

\[IN PROGRESS]

Import the library:
```scala
resolvers += "Spark-tools" at "http://dl.bintray.com/univalence/univalence-jvm"

// replace version with a suitable one
libraryDependencies += "io.univalence" %% "spark-test" % "0.2+245-09a064d9" % Test
```

## Create a DataFrame with a JSON string

We start by importing and extending SparkTest.
```scala
import io.univalence.sparktest.SparkTest
import org.scalatest.FunSuiteLike

class MyTestClass extends FunSuiteLike with SparkTest {}
```

Then, we can create our test:
```scala
class MyTestClass extends FunSuiteLike with SparkTest {
  test("create df with json string") {
    // create df with json string
    val df = dfFromJsonString("{a:1}", "{a:2}")
  }
}
```

## Comparing DataFrames
To compare DataFrames, you can simply call the assertEquals method. It throws an SparkTestError if they are not equal.

For instance, this:
```scala
val dfUT       = Seq(1, 2, 3).toDF("id")
val dfExpected = Seq(2, 1, 4).toDF("id")

dfUT.assertEquals(dfExpected)
```
... throws the following exception:
```
io.univalence.sparktest.SparkTest$ValueError: The data set content is different :

in value at id, 2 was not equal to 1
dataframe({id: 1})
dataframe({id: 2})

in value at id, 1 was not equal to 2
dataframe({id: 2})
dataframe({id: 1})

in value at id, 4 was not equal to 3
dataframe({id: 3})
dataframe({id: 4})
```

## Testing with predicates

One of our test functionality is shouldForAll.

It throws an AssertionException if there are rows that don't match the predicate.

This example:
```scala
val rdd = sc.parallelize(Seq(Person("John", 19), Person("Paul", 17), Person("Emilie", 25), Person("Mary", 5)))
rdd.shouldForAll(p => p.age > 18) // Paul and Mary are too young
```

... will throw this exception:
```scala
java.lang.AssertionError: No rows from the dataset match the predicate. Rows not matching the predicate :
Person(Paul,17) 
Person(Mary,5)
```

Whereas this example:
```scala
val rdd = sc.parallelize(Seq(Person("John", 19), Person("Paul", 52), Person("Emilie", 25), Person("Mary", 83)))
rdd.shouldForAll(p => p.age > 18) // Everyone pass the predicate
```
... will pass!