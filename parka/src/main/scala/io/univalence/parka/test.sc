import org.apache.spark.sql._

val r1 = Row(1, 2, "hello")
val r2 = Row(1, 2, "hello")
val r3 = Row(1, 2, "world")
val r4 = Row(1.1, 2.2, "hello")

r1 == r2
r1 == r3
r1 == r4