package io.univalence.centrifuge

import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Success, Try }

class CircuitBreakerOpenException extends Exception

case class CircuitBreakerConfig(failMax: Int, timeout: Long, firstTimer: Long)

sealed trait CircuitBreaker {
  def run[T](t: ⇒ Try[T]): (Try[T], CircuitBreaker)

  def isOpen: Boolean
  def isClosed: Boolean

  def forceOpen: CircuitBreaker
  def forceClose: CircuitBreaker

}
object CircuitBreaker {

  def createFromConfig(circuitBreakerConfig: CircuitBreakerConfig): CircuitBreaker = {
    Closed(circuitBreakerConfig = circuitBreakerConfig)
  }
}

case class Closed protected[centrifuge] (failCount: Int = 0, circuitBreakerConfig: CircuitBreakerConfig) extends CircuitBreaker {
  override def run[T](t: ⇒ Try[T]): (Try[T], CircuitBreaker) = {

    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    val f = Future(t)

    Try { Await.result(f, circuitBreakerConfig.timeout millis) }.flatten match {
      case t: Failure[_] ⇒ (t, this.copy(failCount = failCount + 1).checkState)
      case s: Success[_] ⇒ (s, this.copy(failCount = 0).checkState)
    }
  }

  protected def checkState: CircuitBreaker = if (failCount >= circuitBreakerConfig.failMax) {
    forceOpen
  } else {
    this
  }

  override def isOpen: Boolean = false
  override def isClosed: Boolean = true

  override def forceOpen: CircuitBreaker = {
    val timer = circuitBreakerConfig.firstTimer
    Open(timer, timer, circuitBreakerConfig)
  }

  override def forceClose: CircuitBreaker = this
}

case class Open protected[centrifuge] (timer: Long, nextTimer: Long, circuitBreakerConfig: CircuitBreakerConfig) extends CircuitBreaker {
  override def run[T](t: ⇒ Try[T]): (Try[T], CircuitBreaker) = {

    if (timer > 0) {
      (Failure(new CircuitBreakerOpenException), this.copy(timer = timer - 1))
    } else {
      import scala.concurrent.duration._
      import scala.concurrent.ExecutionContext.Implicits.global
      val f = Future(t)
      Try { Await.result(f, circuitBreakerConfig.timeout millis) }.flatten match {
        case t: Failure[_] ⇒ (t, {
          val timer = nextTimer * 2
          this.copy(timer, timer)
        })
        case s: Success[_] ⇒ (s, Closed(0, circuitBreakerConfig))
      }
    }

  }

  override def isOpen: Boolean = true
  override def isClosed: Boolean = false

  override def forceOpen: CircuitBreaker = this

  override def forceClose: CircuitBreaker = Closed(0, circuitBreakerConfig)
}
