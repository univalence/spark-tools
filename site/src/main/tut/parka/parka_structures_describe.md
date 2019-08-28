---
layout: docs
section: parka
title: "Parka Structures"
---

# Describe

Describe is a structure which describe a sequence of value in different ways.

## Structure

```scala
case class Describe(count: Long, histograms: Map[String, Histogram], counts: Map[String, Long])
```

### count

**type**: Long

This is one counter, incrementing each time a value is added to the Describe

### histograms

**type**: Map[String, Histogram]

Repartition of values can be describe using histograms, so we provide a map containing them. 

Normally, a Describe will contain one histogram containing each value. However in some cases, we can use multiple histograms to describe a same sequence of values. Imagine if we have a sequence of date, then we can have histograms according to days, months and so on for example.

Another things important about histogram   if your amount of data is too large then the histogram will be converted into a [QTree](https://twitter.github.io/algebird/datatypes/approx/q_tree.html) which is an approximate histogram giving us scalability.

### counts

**type**: Map[String, Long]

This is a map of counter. 

For the moment, there are three kind of counter: nNull, nTrue, nFalse counting the number of null value, the number of true and the number of false added to the Describe.

## DescribeByRow

```scala
case class DescribeByRow(count: Long, byColumn: Map[String, Describe])
```

A Describe is always a part of a little more complex structure such as DescribeByRow.

We usually have a Describe for each column represented here by the "byColumn" argument so we have created a DescribeByRow case class. We have attached a counter that is, for example, usefull to know how much rows are unique inside both Datasets for our Outer analysis.