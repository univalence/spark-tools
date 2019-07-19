---
layout: docs
section: parka
title: "Parka Serde"
---

# Parka Serde

We saw that we can create Parka Analysis from two Datasets, but you can also import a Parka Analysis from a JSON or import a Parka Analysis to JSON.

## Export to JSON

To generate JSON from Parka Analysis you need to follow the example below:

```scala
import io.univalence.parka.{Parka, ParkaAnalysisSerde}

val pa     = Parka(df1, d2f)("key")
val paJson = ParkaAnalysisSerde.toJson(pa)
```

## Import from JSON

To generate Parka Analysis from JSON you need to follow the example below:

```scala
import io.univalence.parka.{Parka, ParkaAnalysisSerde}

val paJson = """{"datasetInfo":{"left":{"source":[],"nStage":0},"right":{"source":[],"nStage":0}},"result":{"inner":{"countRowEqual":1,"countRowNotEqual":1,"countDiffByRow":[{"key":["n"],"value":1},{"key":[],"value":1}],"byColumn":{"n":{"DeltaLong":{"nEqual":1,"nNotEqual":1,"describe":{"left":{"value":{"neg":null,"countZero":0,"pos":{"_1":0,"_2":2,"_3":2,"_4":{"_1":0,"_2":1,"_3":1,"_4":null,"_5":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null}},"_5":{"_1":1,"_2":1,"_3":1,"_4":{"_1":2,"_2":0,"_3":1,"_4":null,"_5":null},"_5":null}}}},"right":{"value":{"neg":null,"countZero":0,"pos":{"_1":0,"_2":2,"_3":2,"_4":{"_1":0,"_2":1,"_3":1,"_4":null,"_5":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null}},"_5":{"_1":1,"_2":1,"_3":1,"_4":null,"_5":{"_1":3,"_2":0,"_3":1,"_4":null,"_5":null}}}}}},"error":{"neg":{"_1":1,"_2":0,"_3":1,"_4":null,"_5":null},"countZero":1,"pos":null}}}}},"outer":{"countRow":{"left":0,"right":0},"byColumn":{}}}}"""
val pa     = ParkaAnalysisSerde.fromJson(paJson).right.get
```