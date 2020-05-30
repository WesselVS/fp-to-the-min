package fpmin

import java.time.Month
import java.time.YearMonth
import java.util.concurrent.TimeUnit

import fpmin.csv._
import zio.blocking.Blocking
import zio.{App, ExitCode, Schedule, URIO, ZEnv, ZIO}
import zio.clock.Clock
import zio.duration.Duration

object StatsCollector1 extends App {
  def aggregate(covid19: Covid19, month: Month): URIO[Blocking with Clock, Csv] = {
    val daysInMonth = YearMonth.of(2020, month.getValue()).lengthOfMonth()
    ZIO.collectAllSuccessesPar((1 to daysInMonth).map { dayInMonth =>
      for {
        csv <- covid19.unsafeLoad(month.getValue(), dayInMonth)
        retained = csv.retain("country_region", "confirmed", "deaths", "recovered", "active", "province_state")
        grouped = retained.groupBy("province_state", "country_region")
      } yield grouped.sortByReverse(grouped("deaths").int)
    }).map{_.foldLeft(Csv.empty)(_ + _)}
  }

  def printSummary(csv: Csv): Unit = {
    println("-- SUMMARY --")
    println(csv)
  }

  def aggregateAndSummarize(covid19: Covid19, month: Month): ZIO[Blocking with Clock, Throwable, Unit] = {
    val result = aggregate(covid19, month)

    result.map{_.truncate(10)}.tap(csv => ZIO.effect(printSummary(csv))).unit
  }

  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] = {
    val github = new Github()
    val covid19 = new Covid19.Live(github)

    for {
      proc <- aggregateAndSummarize(covid19, Month.APRIL)
        .repeat(Schedule.fixed(Duration.apply(1, TimeUnit.MINUTES))).fork
      _ = println("any key to stop")
      _ = Console.in.readLine()
      _ <- proc.interrupt
    } yield ExitCode.success
  }
}
