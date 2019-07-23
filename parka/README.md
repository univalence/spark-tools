Parka
======================

Parka is a library about data quality of a Datasets in Scala.

It implements DeltaQA for Datasets, comparing two Datasets to each other and notifying differences into Parka Analysis which is an object that contains the comparison’s data..

For more information about what deltaQA is, Here is a talk that explain clearly this concept!


## Table of content

- [Installation](#installation)
- [Usage](#usage)
- [Support](#support)
- [Authors](#authors)
- [License](#license)
- [Dependencies](#dependencies)
- [Links](#links)

## Installation

TO BE CONTINUED

## Usage

The entry of Parka is the Parka Analysis object, this object contains a lot of information about the comparison between two Datasets which is very important for Data Quality.

To get Parka Analysis, first import parka and then generate the analysis from two Datasets as below :

```scala
import io.univalence.parka.Parka

val pa: ParkaAnalysis = Parka(df1, d2f)("key")
``` 

First give the two Datasets to compare to and then column(s) that are keys.
 the console or export it in JSON. 

Here is an example :

```scala
import io.univalence.parka.ParkaPrinter

println(ParkaPrinter.printParkaResult(pa.result))
``` 
## Support

If you have any problem/question don't hesitate to add a new issue.

## Authors

Made with :heart: by Univalence's team.

## License

Parka is licensed under the Apache License, Version 2.0 (the “License”); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Dependencies

* [algebird](https://github.com/twitter/algebird) - 0.13.4
* [magnolia](https://github.com/propensive/magnolia) - 0.10.0
* [jline](https://github.com/jline/jline3) - 3.12.1
* [circe](https://github.com/circe/circe) - 0.11.1

## Links

* [Univalence Web site](https://www.univalence.io/)
* [Documentation](https://univalence.github.io/spark-tools/parka/)
* [Source code](https://github.com/univalence/spark-tools/tree/master/parka/src/main/scala/io/univalence/parka)

:star: Star us on GitHub — it helps!