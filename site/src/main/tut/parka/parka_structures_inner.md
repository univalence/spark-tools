---
layout: docs
section: parka
title: "Parka Structures"
---

# Inner

Inner contains meaningful information about rows with keys on both Datasets.
It means that in both Datasets, there is at least one row with exactly same set of keys. 

## Structure

```scala
case class Inner(countRowEqual: Long,
                 countRowNotEqual: Long,
                 countDeltaByRow: Map[Set[String], DeltaByRow],
                 equalRows: DescribeByRow) {
  @transient lazy val byColumn: Map[String, Delta] = {
    val m = implicitly[Monoid[Map[String, Delta]]]
    m.combineAll(countDeltaByRow.map(_._2.byColumn).toSeq :+ equalRows.byColumn.mapValues(d => {
      Delta(d.count, 0, Both(d, d), Describe.empty)
    }))
  }
}
```

### countRowEqual & countRowNotEqual

**type**: Long

They are two counter which tell you how many rows have exactly the same values by incrementing "countRowEqual" and how many have at least one difference by incrementing "countRowNotEqual".

### countDeltaByRow

**type**: Map[Set[String], DeltaByRow]

Rows with at least one difference are are attached to the "countDeltaByRow". It is a map which is composed by a set of String as a key. Each key is composed by columns where differences happens.

Nothing is better than an example, imagine the following Datasets :

- Left Dataset :

  | Key           | Col1          | Col2  |
  | ------------- |---------------| ------|
  | key1          | a             | 1     |
  | key2          | b             | 2     |
  | key3          | c             | 3     |
  | key4          | d             | 4     |
 
- Right Dataset :

  | Key           | Col1          | Col2  |
  | ------------- |---------------| ------|
  | key1          | a             | 1     |
  | key2          | x             | 2     |
  | key3          | y             | 4     |
  | key4          | z             | 5     |


In this case, you will obtain two elements in that map, one with the key [Col1] containing information about the line n°2 and one with the key [Col1, Col2] containing information about the line n°3 and n°4. Each of them has a [DeltaByRow](/spark-tools/parka/parka_structures_delta) as a value.

### equalRows

**type**: DescribeByRow

equalRows contains composition of Rows which are exactly equal don't need a DeltaByRow since we will not compare them to each other,  we directly can add them to a [DescribeByRow](/spark-tools/parka/parka_structures_describe).

### byColumn

**type**: Map[String, Delta]

The value "byColumn" is an additional analysis which gives you information in a column oriented manner instead of a row oriented manner. Each column has its own [Delta](/spark-tools/parka/parka_structures_delta) describing how data evolve for that particular row no matter if there is or no a difference.  
