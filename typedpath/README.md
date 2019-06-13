# Typedpath

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

