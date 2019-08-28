---
layout: docs
section: parka
title: "Parka Structures"
---

# Outer

Outer contains meaningful information about added or removed key from the right to the left Datasets.It means that in the set of key appear only in one of both Datasets. 

## Structure

```scala
case class Outer(both: Both[DescribeByRow])
```

### both

**type**: Both[DescribeByRow]

Both[T] is just a structure that contains T two times accessible using both.left or both.right.

In this case, there are two [DescribeByRow](/spark-tools/parka/parka_structures_describe), one for the left Datasets and one for the right Datasets. We don't need any [DeltaByRow](/spark-tools/parka/parka_structures_delta) in that case because the key appear only in one Dataset so there isn't any comparison.
