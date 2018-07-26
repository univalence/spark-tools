package io.univalence.centrifuge.util

//from https://gist.github.com/alexandru/623fe6c587d73e89a8f14de284ca1e2d

import monix.eval.Task
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** Request limiter for APIs that have quotas per second, minute, hour, etc.
  *
  * {{{
  *   // Rate-limits to 100 requests per second
  *   val limiter = TaskLimiter(TimeUnit.SECONDS, limit = 100)
  *
  *   limiter.request(myTask)
  * }}}
  */
final class TaskLimiter(period: TimeUnit, limit: Int) {
  import monix.execution.atomic.Atomic
  import TaskLimiter.State

  @transient private[this] val state = Atomic(State(0, period, 0, limit))

  def request[A](task: Task[A]): Task[A] =
    Task.deferAction { ec ⇒
      val now = ec.currentTimeMillis()
      state.transformAndExtract(_.request(now)) match {
        case None ⇒ task
        case Some(delay) ⇒
          // Recursive call, retrying request after delay
          request(task).delayExecution(delay)
      }
    }
}

object TaskLimiter {
  /** Builder for [[TaskLimiter]]. */
  def apply(period: TimeUnit, limit: Int): TaskLimiter =
    new TaskLimiter(period, limit)

  /** Timestamp specified in milliseconds since epoch,
    * as returned by `System.currentTimeMillis`
    */
  type Timestamp = Long

  /** Internal state of [[TaskLimiter]]. */
  final case class State(window: Long, period: TimeUnit, requested: Int, limit: Int) {
    private def periodMillis =
      TimeUnit.MILLISECONDS.convert(1, period)

    def request(now: Timestamp): (Option[FiniteDuration], State) = {
      val periodMillis = this.periodMillis
      val currentWindow = now / periodMillis

      if (currentWindow != window)
        (None, copy(window = currentWindow, requested = 1))
      else if (requested < limit)
        (None, copy(requested = requested + 1))
      else {
        val nextTS = (currentWindow + 1) * periodMillis
        val sleep = nextTS - now
        (Some(sleep.millis), this)
      }
    }
  }
}