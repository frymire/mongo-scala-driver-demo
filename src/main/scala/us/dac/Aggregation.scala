package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.{equal}
import org.mongodb.scala.model.Aggregates.{filter, group, sort}
import org.mongodb.scala.model.Accumulators.{sum}
import org.mongodb.scala.model.Sorts.{orderBy, descending}

import us.dac.Helpers._


object Aggregation extends App {

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("Aggregation")
  collection.drop().results()

  println("\nAdd a new set of documents...")
  val documents = List(   
        Document("""{ cust_id: "A123", amount: 500, status: "A" }"""),
        Document("""{ cust_id: "A123", amount: 250, status: "A" }"""),
        Document("""{ cust_id: "B212", amount: 200, status: "A" }"""),
        Document("""{ cust_id: "A123", amount: 300, status: "D" }""")
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()

  println("\nQuery for documents with status 'A', then sum the amounts for each customer ID, then sort in descending order...")
  val pipeline1 = Seq(
      filter(equal("status", "A")), // use filter for $match, since match is a reserved word in Scala
      group("$cust_id", sum("total", "$amount")),
      sort(orderBy(descending("total")))      
    ) 
  collection.aggregate(pipeline1).printResults()
  
  // You could also do it with text like this.
//  val pipeline1 = Seq(
//      Document("""{$match: {status: "A"}}"""),
//      Document("""{$group: {_id: "$cust_id", total: {$sum: "$amount"} } }""")
//    )

  
  mongoClient.close()
}
