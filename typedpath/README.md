Utils
======================

[ ![Download](https://api.bintray.com/packages/univalence/univalence-jvm/typedpath/images/download.svg) ](https://bintray.com/univalence/univalence-jvm/typedpath/_latestVersion)

Utils contains mini librairies, it's like a mini monorepository inside our monorepository.

## Installation

TODO

## Usage

### Schema

Schema is a helper that compare precisely differences between two schemas.

```scala
import io.univalence.schema.SchemaComparator

val schema_1: StructType = ???
val schema_2: StructType = ???

// If modifications is empty then there is no differences
val modifications: Seq[SchemaModification] = compareSchema(schema_1, schema_2)

// If there are modifications, throw a SchemaError with details for each differences
assert(schema_1, schema_2)
```


### TypedPath
Typedpath is a set of case classes (Algebraic Data Types) and a StringContext macro to help represented "simple" key in datastructures.

We have the following types : `Key = ArrayKey | FieldKey`  and `KeyOrRoot = Key | Root`. 

If we have the following  data : 
```clojure
{:person {:name "John", :age 12},
 :status "active"}
```
The value `"John"` is at key `person.name`. In Scala we would do the following.

```scala
import io.univalence.typedpath._
import scala.util.Try


val p0:Try[KeyOrRoot] = Key.create("person.name")
//or
val p1:Try[FieldKey] = for {
  p <- FieldKey.createName("person")
  n <- FieldKey.createName("name")
} yield FieldKey(n,FieldKey(p,Root))
//or
val p2:Try[FieldKey] = for {
 p <- FieldKey("person",Root)
 n <- FieldKey("name", p)
} yield n
//or using the macro
val p3:FieldKey = key"person.name"
```

