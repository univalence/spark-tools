# Typedpath

Typedpath is a set of case classes (Algebraic Data Types) and a StringContext macro to help represented "simple" path in datastructures.

We have the following types : `Path = ArrayPath | FieldPath`  and `PathOrRoot = Path | Root`. 

If we have the following  data : 
```clojure
{:person {:name "John", :age 12},
 :status "active"}
```
The value `"John"` is at path `person.name`. In Scala we would do the following.

```scala
import io.univalence.typedpath._
import scala.util.Try


val p0:Try[PathOrRoot] = Path.create("person.name")
//or
val p1:Try[FieldPath] = for {
  p <- FieldPath.createName("person")
  n <- FieldPath.createName("name")
} yield FieldPath(n,FieldPath(p,Root))
//or
val p2:Try[FieldPath] = for {
 p <- FieldPath("person",Root)
 n <- FieldPath("name", p)
} yield n
//or using the macro
val p3:FieldPath = path"person.name"
```

