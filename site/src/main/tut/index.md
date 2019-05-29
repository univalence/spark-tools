---
layout: home
position: 1
section: home
title: "Home"
---

# Spark tools

Spark-tools is a set of tools dedicated to Spark and aims to make the life of data engineers easier.

## Usage

We recommend you to start using spark-tools with spark-test which is the most accessible tool.

Here is an example from A to Z using spark-test :

First import the last version of spark-test using for example SBT :
```
resolvers += "spark-test" at "http://dl.bintray.com/univalence/univalence-jvm"

libraryDependencies += "io.univalence" %% "spark-test" % "0.2+245-09a064d9" % Test
```

Hourra, you can use spark-test for your tests :

¯\\_(ツ)_/¯

## The other tools

* [Spark-test](https://github.com/univalence/spark-tools/tree/master/spark-test/src), tools to easily make tests with Spark
* [Centrifuge](https://github.com/univalence/spark-tools/tree/master/centrifuge), a couple of techniques for data quality on Spark
* [Fenek](https://github.com/univalence/spark-tools/tree/master/fenek), a DSL for *semi-typed* transformation in Scala for Spark
* [Plumbus](https://github.com/univalence/spark-tools/tree/master/plumbus), light misc things for Spark
* [Spark-ZIO](https://github.com/univalence/spark-tools/tree/master/spark-zio), Spark in ZIO environment

## Getting started

