
package us.dac

import concurrent.{Future, Await}
import concurrent.duration._
import concurrent.ExecutionContext.Implicits.global
import util.{Success, Failure}

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson.codecs.Macros.{createCodecProvider, createCodecProviderIgnoreNone}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

// Intentionally not importing the Helpers.scala file here.

/*
 * Demonstrate the explicit use of the asynchronous observer pattern using the MongoDB Scala driver.
 * 
 * Inspiration: 
 * 	 http://whiletrue.do/2017/04/24/some-examples-of-mongo-scala-driver-usage/
 *   https://www.slideshare.net/hermannhueck/reactive-access-to-mongodb-from-scala
 */
object Observer extends App {
  
  println("\nInitialize the database with a set of documents, blocking until it completes...")
  Await.result(DAO.init(), Duration.Inf)

  println("\nDefine an observable for a query that returns the full set of customers...")
  val customersObservable = DAO.getAllCustomers()
  
  println("\nRegister two observers to print the customers (1) at full speed, and (2) throttled...")
  customersObservable.subscribe(new MyObserver[Customer]("FullSpeed"))
  customersObservable.subscribe(new ThrottledCustomerObserver)
  
  println("\nRegister a callback to print all of the documents retrieved by the client as soon as the future completes...")
  val customersFuture = customersObservable.toFuture()
  customersFuture foreach { customers =>  println("\nfutureAllCustomers.foreach()"); customers foreach println }  
  // NOTE: As of Scala 2.12, foreach() is preferred over the deprecated onSuccess(), despite being a misleading name.
  // Alternatively, use onComplete(), but in that case, you should provide a complete function including the error case. 
//  customersFuture onComplete { 
//    case Success(customers) => 
//      println("\nfutureAllCustomers.onComplete()")
//      customers foreach println
//    case Failure(error) => 
//      println("Error: " + error.getMessage)
//  }
    
  println("\nCreate a query to insert another document into the database, and start its execution immediately by subscribing...")
  DAO.insertOne(Customer(5, 999, 'Z')).subscribe(new MyObserver[Completed]("Extra"))

  println("\nBlock until the find-customers query completes, and again print the documents retrieved by the client...")
  Await.result(customersFuture, Duration.Inf) foreach println
  // Here, the foreach is iterating over a Seq[Customers] returned from a Future. On the other hand, confusingly, 
  // Future.foreach() returns a single result in the type of the future. In the case above, that happens to be a 
  // Seq[Customer] that you must then iterate over with a separate foreach(). 

  println("\nRegister another observer after completing the find-customers future to print the amounts for each customer...")
  customersObservable.subscribe(new AmountsObserver)
  
  println("\nClose the database connection...")
  DAO.close()
  println("Database connection closed.")

  println("\nTrying to subscribe after closing the database results in an error...")
  customersObservable.subscribe(new MyObserver[Customer]("TooLate"))  
}


object DAO {
  
  // Specify a codec registry to convert the Customer type to and from BSON.
  val customerCodecProvider = createCodecProviderIgnoreNone[Customer]()
  val codecRegistry = fromRegistries(fromProviders(customerCodecProvider), DEFAULT_CODEC_REGISTRY)

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(codecRegistry)
  val collection: MongoCollection[Customer] = database.getCollection("Observer")

  /** Returns a future that composes drop and insert operations on the Observer collection. */
  def init(): Future[Completed] = {
    
    val customers = List(
          Customer(1, 500, 'A'),
          Customer(2, 250, 'A'),
          Customer(3, 200, 'A'),
          Customer(4, 300, 'D')
        )
        
    // Just to prove it works, define the drop and insert operations in the wrong order. It's okay as long as we compose them 
    // in the correct order, since they don't execute until a Subscription is made via a call to subscribe() or we extract the 
    // result using Await.result().
    val insertFuture = collection.insertMany(customers).toFuture()
    val dropFuture = collection.drop().toFuture() 
    dropFuture flatMap { f => insertFuture }
  }
  
  def getAllCustomers(): FindObservable[Customer] = collection.find()
  
  def insertOne(d: Customer): SingleObservable[Completed] = collection.insertOne(d) 

  def close(): Unit = mongoClient.close()    
}


case class Customer(customerID: Int, amount: Int, status: Char)

class MyObserver[T](val name: String) extends Observer[T] {
  // onSubscribe is already defined to request all of the documents for the observable at once.
  // def onSubscribe(sub: Subscription) = sub.request(Long.MaxValue) 
  def onNext(result: T) = println(s"$name.onNext() -> $result")
  def onComplete() = println(s"$name.onComplete() -> Done!")
  def onError(e: Throwable) = println(s"$name.onError() -> ${e.getMessage}")
}

class ThrottledCustomerObserver extends MyObserver[Customer]("Throttled") {
  
  // Override onSubscribe(), calling Subscription.request(...) to request the number of documents for observable to send.
  override def onSubscribe(sub: Subscription) = { 
    sub.request(2)
    println(s"$name is processing...")
    Thread.sleep(2000)
    println(s"$name is done processing...")
    sub.request(1) 
    // NOTE: If there are more than three documents, a ThrottledCustomerObserver never calls onComplete().
  }
}

class AmountsObserver extends MyObserver[Customer]("Amounts") {
  override def onNext(c: Customer) = println(s"$name.onNext() -> ${c.amount}")
}
