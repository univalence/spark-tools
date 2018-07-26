package io.univalence.centrifuge

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import monix.eval.{Task, TaskCircuitBreaker}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class ExecutionSummary(nbFailure: Long)

object RetryDs {


  private type LocalExecutionStatus = Boolean
  private type ExecutionStat = Long


  def retryDs[A, C, B](in: Dataset[A])
                      (run: A => Try[C])
                      (integrate: (A, Try[C]) => B)
                      (nbGlobalAttemptMax: Int, circuitBreakerMaxFailure: Int = 10)
                      (implicit encoderA: Encoder[A],
                       encoderB: Encoder[B],
                       encoderI: Encoder[(Option[A], LocalExecutionStatus, B)]): (Dataset[B], ExecutionSummary) = {

    import monix.execution.Scheduler.Implicits.global
    Await.result(retryDsWithTask(in)(a => Task(run(a).get))(integrate)(nbGlobalAttemptMax = nbGlobalAttemptMax, circuitBreakerMaxFailure = Some(circuitBreakerMaxFailure)).runAsync, Duration.Inf)
  }

  private def toExecutionStat(les: LocalExecutionStatus): ExecutionStat = if (les) 0 else 1

  private def addExecutionState(es1: ExecutionStat, es2: ExecutionStat): ExecutionStat = es1 + es2

  private def initExecutionSummary(fes: ExecutionStat): ExecutionSummary = ExecutionSummary(fes)

  private def localExecutionFromError(e: Throwable): LocalExecutionStatus = false

  private val successExecution: LocalExecutionStatus = true

  def retryDsWithTask[A, C, B](in: Dataset[A])
                              (run: A => Task[C])
                              (integrate: (A, Try[C]) => B)
                              (nbGlobalAttemptMax: Int, circuitBreakerMaxFailure: Option[Int] = Option(10))
                              (implicit
                               encoderA: Encoder[A],
                               encoderB: Encoder[B],
                               encoderI: Encoder[(Option[A], LocalExecutionStatus, B)]
                              ): Task[(Dataset[B], ExecutionSummary)] = {


    type M = (Option[A], LocalExecutionStatus, B)

    def newCircuitBreaker: Option[TaskCircuitBreaker] = circuitBreakerMaxFailure.map(n => TaskCircuitBreaker(n, Duration(1, TimeUnit.MINUTES)))


    def aToM(a: A, endo:Task[C] => Task[C]): M = {
      import monix.execution.Scheduler.Implicits.global
      val task: Task[C] = endo(run(a))

      val tried: Try[C] = Try(Await.result(task.runAsync, Duration.Inf))

      tried match {
        case Failure(e) => (Some(a), localExecutionFromError(e), integrate(a, tried))
        case Success(_) => (None, successExecution, integrate(a, tried))
      }
    }

    def loopTheLoop(mAndEs: (Dataset[M], ExecutionSummary), attemptRemaining: Int): Task[(Dataset[M], ExecutionSummary)] = {
      if (mAndEs._2.nbFailure == 0 || attemptRemaining <= 0)
        Task.pure(mAndEs)
      else {
        Task {
          val (ds, _) = mAndEs
          val newDs: Dataset[(Option[A], LocalExecutionStatus, B)] = ds.mapPartitions(iterator => {
            val circuitBreaker = newCircuitBreaker
            iterator.map({
              case (Some(a), _, _) => aToM(a, x => circuitBreaker.fold(x)(_.protect(x)))
              case x => x
            })
          })
          newDs.persist()
          val es: ExecutionSummary = dsToEs(newDs)
          ds.unpersist()
          (newDs, es)
        }.flatMap(x => loopTheLoop(x, attemptRemaining - 1))
      }
    }

    def dsToEs(ds: Dataset[M]): ExecutionSummary = {
      import ds.sparkSession.implicits._
      initExecutionSummary(ds.map(_._2).rdd.map(toExecutionStat).reduce(addExecutionState))
    }

    Task({
      val init: Dataset[M] = {
        in.mapPartitions(iterator => {
          val circuitBreaker = newCircuitBreaker
          iterator.map(x => aToM(x,t => circuitBreaker.fold(t)(_.protect(t))))
        })
      }

      init.persist()
      (init, dsToEs(init))
    }).flatMap(x => loopTheLoop(x, nbGlobalAttemptMax - 1)).map({ case (a, b) => (a.map(_._3), b) })

  }
}