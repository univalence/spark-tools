package io.univalence.centrifuge

import io.univalence.Toto
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite
import monix.eval.Task
import monix.execution.CancelableFuture

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class ExecutorTest extends FunSuite {

  def error: Nothing = throw new Exception("run error")





  test("spark") {

    val conf: SparkConf = new SparkConf()
    conf.setAppName("yo")
    conf.set("spark.sql.caseSensitive", "true")
    conf.setMaster("local[2]")

    implicit val ss: SparkSession = SparkSession.builder.config(conf).getOrCreate
    import ss.implicits._

    val totoes = Seq(Toto("a", 1), Toto("b", 2))
    val ds = ss.createDataset(totoes)

    assert(RetryDs.retryDs(ds)(run = x ⇒ Try(2 -> 1))({ case (a, Success((2, 1))) ⇒ a })(nbGlobalAttemptMax = 1000)._1.collect().toSeq == totoes)

    val startDate: Long = System.currentTimeMillis()

    val wait = 5000

    val res = RetryDs.retryDs(ds)(
      run = x ⇒ if (System.currentTimeMillis() - startDate > wait) Try(2 -> 1) else Failure(new Exception("too soon"))
    )({ case (a, _) ⇒ a })(1000)

    assert(res._1.collect().toSeq == totoes)

    assert(System.currentTimeMillis() - startDate > wait)
  }


  test("second implementation should work on pure function") {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("yo")
    conf.set("spark.sql.caseSensitive", "true")
    conf.setMaster("local[2]")

    implicit val ss: SparkSession = SparkSession.builder.config(conf).getOrCreate
    import ss.implicits._

    import monix.execution.Scheduler.Implicits.global

    val t  =RetryDs.retryDsWithTask(ss.createDataset(Seq(1, 2, 3)))(x => Task(x + 1))({ case (a, Success(c)) => c })(1)


    assert(Await.result(t.runAsync,Duration.Inf)._1.collect().toList == List(2,3,4))



  }


}
