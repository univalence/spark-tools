---
layout: docs
section: spark-zio
title: "Getting started"
---

# {{page.title}}

## What you will need

If you want to use Spark-ZIO, you will need these:

```scala
val runtime: DefaultRuntime = new DefaultRuntime {}
val ss: SparkSession        = SparkSession.builder.master("local[*]").getOrCreate()
val sparkEnv: SparkZIO      = new SparkZIO(ss)
```

You can obviously change the configuration for your runtime and your SparkSession.

Now that you wield the power of Spark and ZIO together, let's make a simple local Spark job:

```scala
import io.univalence.sparkzio.SparkEnv.TaskS
val program: TaskS[Unit] = for {
  df <- sparkEnv.read.parquet(pathToParquet)
  _  <- Task(df.show())
} yield ()
runtime.unsafeRun(program)
```
