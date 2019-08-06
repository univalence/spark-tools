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
         Differences by sequence of keys: Key (value) has 10 occurrences
         Delta by key:
             value:
                 Number of similarities: 0
                 Number of differences: 10
                 Describes:
                     Left:                                      Right:                                  
                         Histogram:                                 Histogram:                          
                             100,00 | oooooooooooooooooooooo 2           0,00 | ooooooooooo            1
                             100,20 | oooooooooooooooooooooo 2          18,20 | oooooooooooooooooooooo 2
                             100,40 | oooooooooooooooooooooo 2          36,40 | oooooooooooooooooooooo 2
                             100,60 | oooooooooooooooooooooo 2          54,60 | oooooooooooooooooooooo 2
                             100,80 | oooooooooooooooooooooo 2          72,80 | oooooooooooooooooooooo 2
                             101,00 | oooooooooooooooooooooo 2          91,00 | ooooooooooo            1
                 value:
                      10,00 | ooooooooooo            1
                      28,20 | oooooooooooooooooooooo 2
                      46,40 | oooooooooooooooooooooo 2
                      64,60 | oooooooooooooooooooooo 2
                      82,80 | oooooooooooooooooooooo 2
                     101,00 | ooooooooooo            1
     Outer:
         Number of unique row on the left dataset: 0
         Number of unique row on the right dataset: 1
         Describe by key:
             value:
                 Describes:
                     Left:  Right:                                   
                                Histogram:                           
                                    100,00 | oooooooooooooooooooooo 1
                                    100,20 |                        0
                                    100,40 |                        0
                                    100,60 |                        0
                                    100,80 |                        0
                                    101,00 |                        0
```