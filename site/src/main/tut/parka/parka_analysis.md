---
layout: docs
section: parka
title: "Parka Analysis"
---

# Parka Analysis

Everything in Parka start by generating a Parka Analysis case class.

This class contains all the information about the data quality of our data between our two Datasets.

To obtain our Parka Analysis we first need two Datasets to compare to each other. Both Datasets one or more keys 

Then just import Parka and use it as below :

```scala
import io.univalence.parka.Parka

val pa: ParkaAnalysis = Parka(df1, d2f)("key")
```

As we said before, a Parka Analysis contains a lot of information.

````scala
case class ParkaAnalysis(datasetInfo: Both[DatasetInfo], result: ParkaResult)
````

## Dataset Info

DatasetInfo isn't implemented yet.

## Parka Result

ParkaResult contains two type of analysis, an inner and an outer analysis.

The difference between them is simple, if the Datasets on the right and the Datasets on the left have a row with the same key(s) then this row will be part of the inner analysis. 
If there is a row with a particular key only on the right or on the left Datasets then this row will be part of the outer analysis.