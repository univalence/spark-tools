package io.univalence.centrifuge

import java.util.concurrent.TimeUnit

import io.univalence.Toto
import monix.eval.Task
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class ExecutorTest extends FunSuite {

  val conf: SparkConf = new SparkConf()
  conf.setAppName("yo")
  conf.set("spark.sql.caseSensitive", "true")
  conf.setMaster("local[2]")

  val ss: SparkSession = SparkSession.builder.config(conf).getOrCreate

  import ss.implicits._

  //TODO too slow
  ignore("spark") {
    val totoes: Seq[Toto] = Seq(Toto("a", 1), Toto("b", 2))
    val ds                = ss.createDataset(totoes)

    assert(
      RetryDs
        .retryDs(ds)(run = x => Try(1))({ case (a, Success(1)) => a })(nbGlobalAttemptMax = 1000)
        ._1
        .collect()
        .toSeq == totoes
    )

    val startDate: Long = System.currentTimeMillis()

    val wait = 5000

    val (resDs, ex) = RetryDs.retryDs(ds)(
      run = x =>
        if (System.currentTimeMillis() - startDate > wait) Try(2 -> 1)
        else Failure(new Exception("too soon"))
    )({ case (a, _) => a })(1000)

    assert(resDs.collect().toSeq == totoes)
    assert(ex.nbFailure == 0)

    assert(System.currentTimeMillis() - startDate > wait)
  }

  test("implementation should work on pure function") {
    val (ds, _) =
      RetryDs.retryDs(ss.createDataset(Seq(1, 2, 3)))(x => Try(x + 1))({
        case (a, Success(c)) => c
      })(1)

    assert(ds.collect().toList == List(2, 3, 4))
  }

  test("circuit breaker should the stop calling the task for current partition after 10 executions in error") {

    val ds = ss.createDataset(1 to 19).coalesce(1)

    RetryDs.retryDs(ds)(CircuitBreakerMutable.f)({ case (a, _) => a })(1000, circuitBreakerMaxFailure = 10)

    ////println(((CircuitBreakerMutable.calls)

    val res = (1 to 10) ++ (1 to 10) ++ (11 to 19) ++ (11 to 19)

    assert(CircuitBreakerMutable.calls == res)

  }

  test("should work with a timeout") {

    val ds = ss.createDataset(List(1, 2, 5, 10, 100, 200, 500, 1000))

    val t =
      RetryDs.retryDsWithTask(ds)(x => Task(Thread.sleep(x)).timeout(Duration(10, TimeUnit.MILLISECONDS)))(
        (x, y) => (x, y.isSuccess)
      )(2, None)

    import monix.execution.Scheduler.Implicits.global
    val res = Await.result(t.runAsync, Duration.Inf)._1.collect().toList

    assert(res.forall({
      case (a, true)  => a <= 10
      case (a, false) => a > 10
    }))

  }

}

object CircuitBreakerMutable {

  @transient var calls: Seq[Int] = Vector.empty

  @transient var shouldPassNext: Set[Int] = Set.empty

  def f(i: Int): Try[Int] = {

    calls = calls :+ i
    val res = if (shouldPassNext(i)) {
      Try(i)
    } else {
      shouldPassNext = shouldPassNext + i
      Failure(new Exception("not available"))
    }
    res
  }

}
