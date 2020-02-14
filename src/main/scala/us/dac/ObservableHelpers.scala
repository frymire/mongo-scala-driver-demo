package us.dac

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.mongodb.scala._

object ObservableHelpers {

  trait ImplicitObservable[T] {
    
    val observable: Observable[T]
    def converter(t: T): String

    def result(): T = Await.result(observable.head(), Duration.Inf)
    def results(): Seq[T] = Await.result(observable.toFuture(), Duration.Inf)
    def printResults(): Unit = results() map { converter(_) } foreach println
  }

  implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override def converter(d: Document): String = d.toJson
  }

  implicit class GenericObservable[T](val observable: Observable[T]) extends ImplicitObservable[T] {
    override def converter(t: T): String = t.toString
  }
}
