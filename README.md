[![Build Status](https://travis-ci.org/UNIVALENCE/centrifuge.png?branch=master)](https://travis-ci.org/UNIVALENCE/centrifuge)
# centrifuge
Data quality tools for Big Data

* Scala 2.11.7
* JDK 8
* Spark 2.1.1

See this [video](https://www.youtube.com/watch?v=t24sUF2zWLY) for in-depth coverage of the project

### Quick start
centrifuge will be published to Maven Central soon, so when it is done you will be able to get centrifuge with adding this to your `build.sbt`:
    
    val sparkV = "2.1.1"
    
    libraryDependencies ++= Seq(
      "io.univalence" %% "centrifuge" % "0.1"
    )
    
###### Using Annotations
We import relevant libraries and define 3 case classes
    
    package io.univalence.centrifuge.sql
    
    import io.univalence.centrifuge.{Annotation, AnnotationSql, Result}
    import org.apache.spark.sql.SparkSession
    import io.univalence.centrifuge.implicits._
    import ss.implicits._
    
    case class Person(name: String, age: Int)
    case class PersonWithAnnotations(name: String, age: Int, annotations: Seq[Annotation])
    case class BetterPerson(name:String,age:Option[Int])

Here we create 3 instances of Person

    val joe = Person("Joe", 14)
    val timmy = Person("Timmy",8)
    val invalid = Person("",-1)    

Making a Dataset from the 3 Person we just created

    val ds = ss.sparkContext.makeRDD(Seq(joe,timmy,invalid)).toDS()
    ds.createOrReplaceTempView("personRaw")
    
bla
    
    ss.registerTransformation[Int,Int]("checkAge", {
      case i if i < 0 => Result.fromError("INVALID_AGE")
      case i if i < 13 => Result.fromWarning(i, "UNDER_13")
      case i if i > 140 => Result.fromError("OVER_140")
      case i => Result.pure(i)
    })
  
Basic select
    
    ss.sql("select name, checkAge(age) as age, age as ageRaw from personRaw").show(false)

Result

    +-----+----+------+
    |name |age |ageRaw|
    +-----+----+------+
    |Joe  |14  |14    |
    |Timmy|8   |8     |
    |     |null|-1    |
    +-----+----+------+ 

This time using BetterPerson (a Person with Option)

    ss.sql("select name, checkAge(age) as age from personRaw").as[BetterPerson].collect().foreach(println)
    
As expected
    
    BetterPerson(Joe,Some(14))
    BetterPerson(Timmy,Some(8))
    BetterPerson(,None)
    
Now we include Annotations

    ss.sql("select name, checkAge(age) as age from personRaw").includeAnnotations.show(false)
    
Result

    +-----+----+--------------------------------------------+
    |name |age |annotations                                 |
    +-----+----+--------------------------------------------+
    |Joe  |14  |[]                                          |
    |Timmy|8   |[[UNDER_13,age,WrappedArray(age),false,1]]  |
    |     |null|[[INVALID_AGE,age,WrappedArray(age),true,1]]|
    +-----+----+--------------------------------------------+

We now have annotations for each Person in the DataSet.

Let's take a look at what an Annotation is:

    Annotation(message: String,
               onField: Option[String] = None,
               fromFields: Vector[String] = Vector.empty,
               isError: Boolean,
               count: Long)
* **message** contains a relevant information
* **onField** is the name of the field the message is from
* **fromFields** is
* **isError** is **true** if the data is not within acceptable values else it's **false**
* **count** is the number of time the value has appeared for the same field

### TODO / Roadmap
Integrate a reworked version of Excelsius in order to have a better DataFrame.show()

To go from 

    +-----+----+--------------------------------------------+
    |name |age |annotations                                 |
    +-----+----+--------------------------------------------+
    |Joe  |14  |[]                                          |
    |Timmy|8   |[[UNDER_13,age,WrappedArray(age),false,1]]  |
    |     |null|[[INVALID_AGE,age,WrappedArray(age),true,1]]|
    +-----+----+--------------------------------------------+

To

    ++-----+----+------------------------------------------------+
    +|name |age |annotations                                     |
    +|     |    |message     |onField |fromFields |isError |count|
    ++-----+----+------------+--------+-----------+--------+-----+
    +|Joe  |14  |                  ---                           |
    +|Timmy|8   |UNDER_13    |age     |age        |false   |1    |
    +|     |null|INVALID_AGE |age     |age        |true    |1    |
    +|  ------  |EMPTY_STRING|name    |name1      |true    |1    |
    +|  ------  |     ---------       |name2      |    ----      |
    ++-----+----+------------+--------+-----------+--------+-----+

"-" is a placeholder and a better symbol/ascii art will be used
## License
centrifuge is licensed under the Apache License, Version 2.0 (the “License”); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.