---
layout: docs
section: parka
title: "Outer Analysis"
---

# Outer Analysis

Outer contains meaningful information about added or removed key from the right to the left Datasets.

It contains the number of unique key's rows for each Datasets and additional information about these rows for each column. 

Basically we provide a map where keys are column names (excepted keys) and values are a Describe object containing repartition information which is slightly different according to the DataType of the column.
