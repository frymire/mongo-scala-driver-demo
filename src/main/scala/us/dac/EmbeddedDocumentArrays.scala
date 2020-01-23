package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import us.dac.Helpers._

object EmbeddedDocumentArrays extends App {

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("EmbeddedDocumentArrays")
  collection.drop().results()
    
  println("\nAdd a new set of documents with embedded document arrays...")
  val documents = List(      
        Document("""{ item: 'journal', instock: [ { warehouse: 'A', qty: 5 }, { warehouse: 'C', qty: 15 } ] }"""),
        Document("""{ item: 'notebook', instock: [ { warehouse: 'C', qty: 5 } ] }"""),
        Document("""{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 15 } ] }"""),
        Document("""{ item: 'planner', instock: [ { warehouse: 'A', qty: 40 }, { warehouse: 'B', qty: 5 } ] }"""),
        Document("""{ item: 'postcard', instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }""")
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()
  
  println("\nQuery for the embedded document \"{ warehouse: 'B', qty: 15 }\" within the instock array field...")
  collection.find(equal("instock", Document("warehouse" -> "B", "qty" -> 15))).printResults()

  println("\nQuery for the embedded document \"{ qty: 15, warehouse: 'B' }\", with field order reversed (no matches)...")
  collection.find(equal("instock", Document("qty" -> 15, "warehouse" -> "B"))).printResults()
  
  println("\nQuery the instock array field for embedded documents with at least one qty field greater than 30...")
  collection.find(gt("instock.qty", 30)).printResults()

  println("\nReturn documents where the first embedded document in the instock array field has a qty greater than 30...")
  collection.find(gt("instock.0.qty", 30)).printResults()

  println("\nReturn documents where a single embedded document in instock has a qty of 5 and warehouse A (works with order reversed)...")
  collection.find(elemMatch("instock", Document("qty" -> 5, "warehouse" -> "A"))).printResults()
  
  println("\nReturn documents where any combination of embedded documents in instock has a qty of 5 and warehouse A...")
  collection.find(and(equal("instock.qty", 5), equal("instock.warehouse", "A"))).printResults()

  println("\nReturn documents where a single embedded document in instock has a qty between 10 and 20...")
  collection.find(elemMatch("instock", Document("""{ qty: { $gt: 10, $lt: 20 } }"""))).printResults()
  
  println("\nReturn documents where a document in instock has a qty greater than 10 and one (possibly different) has a qty less than 20...")
  collection.find(and(gt("instock.qty", 10), lt("instock.qty", 20))).printResults()

  mongoClient.close()
}
