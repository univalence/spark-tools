package io.univalence

import org.scalatest.FunSuite
import io.univalence.centrifuge.{ CircuitBreaker, CircuitBreakerConfig, Open }

import scala.util.{ Failure, Try }

class CircuitBreakerTest extends FunSuite {

  val conf = CircuitBreakerConfig(
    failMax = 2,
    timeout = 10,
    firstTimer = 1
  )

  def fail = Failure(new Exception)

  def success[A](a: ⇒ A) = Try(a)

  def slow[A](sleep: Long)(a: ⇒ A) = Try { Thread.sleep(sleep); a }

  test("circuit breaker") {

    val cb = CircuitBreaker.createFromConfig(conf)

    assert(cb.run(fail)._2.run(fail)._2.isOpen)

    assert(cb.run(slow(11)(1))._2.run(fail)._2.isOpen)

    assert(cb.forceOpen.isOpen)

    assert(cb.forceOpen.run(success(true))._1.isFailure)

    val closingOpen = cb.forceOpen.run(success(true))._2.run(success(true))

    assert(closingOpen._1.get)
    assert(closingOpen._2.isClosed)

    val stayClosed = cb.forceOpen.run(fail)._2.run(fail)._2

    assert(stayClosed.isOpen)
    assert(stayClosed.asInstanceOf[Open].timer == 2)

    val stayClosed2 = stayClosed.run(fail)._2.run(fail)._2.run(fail)._2

    assert(stayClosed2.asInstanceOf[Open].timer == 4)

    var executed = false

    val closed = cb.forceOpen.run(success(executed = true))

    assert(closed._1.isFailure)
    assert(!executed)

  }
}
