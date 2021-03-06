Parka
======================

[ ![Download](https://api.bintray.com/packages/univalence/univalence-jvm/parka/images/download.svg) ](https://bintray.com/univalence/univalence-jvm/parka/_latestVersion)

Parka is a library about data quality of a Datasets in Scala.

It implements DeltaQA for Datasets, comparing two Datasets to each other and notifying differences into Parka Analysis which is an object that contains the comparison’s data..

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

A stable version isn't available yet.

### Latest version

If you want to get the very last version of this library you can still download it using bintray here : https://bintray.com/univalence/univalence-jvm/parka

Here is an example using ```version 0.3+79-4936e981``` that work with ```scala 2.11.X```:

```scala
resolvers += "parka" at "http://dl.bintray.com/univalence/univalence-jvm"
libraryDependencies += "io.univalence" %% "parka" % "0.3+79-4936e981"
```

## Usage

The entry of Parka is the Parka Analysis object, this object contains all the information about the comparison between two Datasets.

To get a Parka Analysis, first import parka and then generate the analysis from two Datasets as below :

```scala
import io.univalence.parka.Parka

val pa: ParkaAnalysis = Parka(df1, d2f)("key")
//or

val pa: ParkaAnalysis = Parka.withConfig(nPartition = 500)(df1, df2)("key1", "key2")

``` 

First give the two Datasets to compare to and then column(s) that are keys. then print the result in the console or export it in JSON. 

Here is an example :

```scala
import io.univalence.parka.Printer

println(Printer.printParkaResult(pa.result))
``` 
## Support

If you have any problem/question don't hesitate to add a new issue.

## Authors

Made with :heart: by Univalence's team.

## License

Parka is licensed under the Apache License, Version 2.0 (the “License”); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Dependencies

* [algebird](https://github.com/twitter/algebird) - 0.13.4 --> Approximate data structure
* [magnolia](https://github.com/propensive/magnolia) - 0.10.0 --> Generic derivation for typeclasses
* [jline](https://github.com/jline/jline3) - 3.12.1 --> Console's size
* [circe](https://github.com/circe/circe) - 0.11.1 --> Json serialization
* [spark-test](https://github.com/univalence/spark-tools/tree/master/spark-test) - current --> Dataframe's testing tool

## Links

* [Univalence Web site](https://www.univalence.io/)
* [Microsite](https://univalence.github.io/spark-tools/parka/)
* [Source code](https://github.com/univalence/spark-tools/tree/master/parka/src/main/scala/io/univalence/parka)
* [Video](https://www.youtube.com/watch?v=t24sUF2zWLY) - DeltaQA introduction between [14:25](http://www.youtube.com/watch?v=t24sUF2zWLY&t=14m25s) and [28:10](http://www.youtube.com/watch?v=t24sUF2zWLY&t=28m10s)

:star: Star us on GitHub — it helps!
