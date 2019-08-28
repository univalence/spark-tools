---
layout: docs
section: parka
title: "Parka Structures"
---

# Delta

Delta is a structure which describe a sequence of value from both Datasets accompaninied by an error description.

## Structure

```scala
case class Delta(nEqual: Long, nNotEqual: Long, describe: Both[Describe], error: Describe)
```

### nEqual & nNotEqual

**type**: Long

They are two counter which tell you how many rows have exactly the same values by incrementing "nEqual" and how many have at least one difference by incrementing "nNotEqual".

That different from "countRowEqual" and "countRowNotEqual" in Inner since difference or not is usually corresponding to one or a set of columns not every column.

### describe

**type**: Both[Describe]

Two Describes for values in each Datasets since both are different.

### error

**type**: Describe

A Describe for the difference between the left and the right Datasets. Generally "right_value - left_value", for special case such as string or array, we use levenshtein to calculate the distance for the moment.

## DeltaByRow

```scala
case class DeltaByRow(count: Long, byColumn: Map[String, Delta])
```

When there is a difference between two rows, we use a DeltaByRow instead of a [DescribeByRow](/spark-tools/parka/parka_structures_describe). It allows us to get a Describe for both Datasets since they are different but also a describe for our error.

DeltaByRow are only available inside Inner analysis and that is totally logic. In outer you can't compare rows since they don't have any equivalent in the other side.

