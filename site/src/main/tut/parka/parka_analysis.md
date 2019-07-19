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

### Outer Analysis

Outer contains meaningful information about added or removed key from the right to the left Datasets.

It contains the number of unique key's rows for each Datasets and additional information about these rows for each column. 

Basically we provide a map where keys are column names (excepted keys) and values are a Describe object containing repartition information which is slightly different according to the DataType of the column.

### Inner Analysis

Inner contains meaningful information about rows with keys on both Datasets.

It contains the number of similar rows and different rows. Also for each different keys the number of difference and a big part of Inner is about comparison between similar rows for each columns.

This comparison contains the number of similar rows and different rows for a particular key and the repartition of both Datasets for this key.