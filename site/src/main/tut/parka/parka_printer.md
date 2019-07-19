---
layout: docs
section: parka
title: "Parka Printer"
---

# Parka Printer

When you obtain your Parka Analysis, you can analyse it directly on the console using the Parka Printer.

```scala
import io.univalence.parka.{Parka, ParkaPrinter}

val pa = Parka(df1, d2f)("key")
println(ParkaPrinter.printParkaResult(pa.result))
```

On a simple Parka Analysis, you can obtain the following result :

```text
 Parka Result:
     Inner:
         Number of equal row: 0
         Number of different row: 10
         Differences by sequence of keys:
             Key (value) has 10 occurrences
         Delta by key:
             value:
                     Number of similarities: 0
                     Number of differences: 10
                     Describes:
                          Left:                                      Right:                              
                              Histogram:                                 Histogram:                      
                              100,00 |ooooooooooooooooooooooo 5           0,00 |oooooooooooo 1           
                              100,20 |ooooooooooooooooooooooo 5          18,20 |ooooooooooooooooooooooo 2
                              100,40 |ooooooooooooooooooooooo 5          36,40 |ooooooooooooooooooooooo 2
                              100,60 |ooooooooooooooooooooooo 5          54,60 |ooooooooooooooooooooooo 2
                              100,80 |ooooooooooooooooooooooo 5          72,80 |ooooooooooooooooooooooo 2
                              101,00 |ooooooooooooooooooooooo 5          91,00 |oooooooooooo 1           
                         Error's histogram:
                          10,00 |oooooooooooo 1
                          28,20 |ooooooooooooooooooooooo 2
                          46,40 |ooooooooooooooooooooooo 2
                          64,60 |ooooooooooooooooooooooo 2
                          82,80 |ooooooooooooooooooooooo 2
                         101,00 |oooooooooooo 1
     Outer:
         Number of unique row on the left dataset: 0
         Number of unique row on the right dataset: 1
         Describe by key:
             value:
                 Describes:
                      Left:                   Right:         
                          Empty describe          Histogram: 
                                                  100,00 |o 0
                                                  100,20 |o 0
                                                  100,40 |o 0
                                                  100,60 |o 0
                                                  100,80 |o 0
                                                  101,00 |o 0
```