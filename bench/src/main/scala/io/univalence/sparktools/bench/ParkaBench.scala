package io.univalence.sparktools.bench

import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class ParkaBench {

  @Setup
  def setup(): Unit = {}

  @Benchmark
  def b1(): Unit = {}

  @Benchmark
  def b2(): Unit = {}

}
