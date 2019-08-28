---
layout: docs
section: parka
title: "Parka Structures"
---

# Parka Structures

This section refers to the structures of a Parka Analysis. As we said before, a Parka Analysis contains a lot of information, this Structure Part will help you to understand each kind of structures present on a Parka Analysis.

Everything start with a Parka Analysis as below :

````scala
case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)
````

The left part, datasetInfo, isn't implemented yet.

The right part, result, contains two type of analysis, an inner and an outer analysis. 

The difference between them is simple: 
- if the Datasets on the right and the Datasets on the left have a row with the same key(s) then this row will be part of the inner analysis. 
- if there is a row with a particular key only on the right or on the left Datasets then this row will be part of the outer analysis.

## Global data structure

Here is the global structure of a Parka Analysis:

- datasetInfo: Both[DatasetInfo]
   - Not Implemented
- resul: ParkaResult
    - inner: Inner
        - countRowEqual: Long
        - countRowNotEqual: Long
        - countDeltaByRow: Map[Set[String], DeltaByRow]
        - equalRows: DescribeByRow
        - byColumn: Map[String, Delta]
    - outer: Outer
        - both: Both[DescribeByRow]

## Related link(s)

https://github.com/univalence/spark-tools/blob/master/parka/src/main/scala/io/univalence/parka/Structure.scala
