Spark-Test
======================

Spark-Test is a library to help developers test their Spark applications.

Our goal is to provide you with tools that can help you test your code fast and in an easy way, precisely pointing out why something bad occurred with your Spark collections.

You just have to extend SparkTest to have all those tools!

## Table of content

- [Installation](#installation)
- [Usage](#usage)
- [Support](#support)
- [Authors](#authors)
- [License](#license)
- [Dependencies](#dependencies)
- [Links](#links)

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

## Usage

### Implicit Spark Session

Spark-Test provides an implicit Spark Session, it means that you don't have to define a Spark Session for your tests. You can immediately  

### Create a DataFrame with a JSON string

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

### Comparing DataFrames
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

## Support

If you have any problem/question don't hesitate to add a new issue.

## Authors

Made with :heart: by Univalence's team.

## License

Spark-Test is licensed under the Apache License, Version 2.0 (the “License”); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Dependencies

* [typedpath](https://github.com/univalence/spark-tools/tree/master/typedpath) - current

## Links

* [Univalence Web site](https://www.univalence.io/)
* [Documentation](https://www.javadoc.io/doc/io.univalence/spark-test_2.11)
* [Microsite](https://univalence.github.io/spark-tools/spark-test/)
* [Source code](https://github.com/univalence/spark-tools/tree/master/spark-test)
* [Article EN](https://blog.univalence.io/tests-with-spark-how-to-keep-our-heads-above-water/) | [FR](https://blog.univalence.io/les-tests-avec-spark-sortir-la-tete-de-leau/)

:star: Star us on GitHub — it helps!