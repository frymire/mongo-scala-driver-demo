package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.{WriteModel, InsertOneModel, UpdateOneModel, DeleteOneModel, ReplaceOneModel, BulkWriteOptions}

import us.dac.Helpers._

// Before running, call "mongod --dbpath C:\Users\Mark.E.Frymire\Documents\MongoDB" or equivalent.
object CRUD extends App {
  
  // Connect to the "MongoScalaDriverDemo" database and make a new collection called "CRUD".
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("CRUD")
  collection.drop().results()
  
  // You can compose BSON documents like this.
  val doc1 = Document("AL" -> "Alabama")
  val doc2 = doc1 + ("AK" -> "Alaska")
  val doc3 = doc2 ++ Document("AR" -> "Arkansas", "AZ" -> "Arizona")
  println(s"Document 1: $doc1")
  println(s"Document 2: $doc2")
  println(s"Document 3: $doc3")  
  
  // Convert between documents and JSON strings like this.
  println(s"\nDocument 3 as JSON: ${doc3.toJson()}")
  println(s"Print just the value of the \'AL\' field: ${doc3("AL")}")
  println(s"A new document from a JSON string: ${Document("""{"IL": "Illinois", "VA": "Virginia"}""")}")
  
  println(s"A document with an array: ${Document("Hello" -> "World", "someNumbers" -> List(2,3,4))}")
    
  println("\nAdd a document...")
  collection.insertOne(doc3).results()
  collection.find.first().printResults()
  println(s"Number of documents (should be 1): ${collection.countDocuments().result()}")
  
  println("\nInsert 10 new documents, but without incurring network I/O...")
  val documents = (1 to 10) map { i => Document("i" -> i) }
  val insertObservable = collection.insertMany(documents)
  println(s"# documents after inserting 10 (still 1, since no network I/O): ${collection.countDocuments().result()}")
  
  println("\nForce it to execute by using result() from the Helpers trait...")
  insertObservable.results()
  println(s"Number of documents (should be 11): ${collection.countDocuments().result()}")
  
  println("\nPrint all of the results...")
  collection.find().printResults()
  
  println("\nQuery for a specific document...")
  collection.find(equal("i", 7)).printResults()
  
  println("\nReturn documents with no i field...")
  collection.find(exists("i", false)).printResults()
  
  println("\nQuery for a range of documents...")
  val rangeQuery = and(gt("i", 4), lte("i", 7))
  collection.find(rangeQuery).printResults()  
  
  println("\nSort query results in descending order...")
  collection.find(rangeQuery).sort(descending("i")).printResults()  
    
  println("\nAggregate to make a field that is 10*i, for records with i > 7...")
  val pipeline1 = Seq(
      filter(gt("i", 7)),
      project( Document("""{ITimes10: {$multiply: ["$i", 10]}}""") )
    ) 
  collection.aggregate(pipeline1).printResults()
  
  // To accumulate over the whole document set, group with a groupBy parameter of null.
  println("\nSum the values of the i field...")
  val pipeline2 = List(group(null, sum("total", "$i"), avg("mean", "$i")))
  collection.aggregate(pipeline2).printResults()
  
  println("\nUpdate one record...")
  collection.updateOne(equal("i", 10), set("i", 110)).printResults()
  collection.find().printResults()
  
  println("\nUpdate multiple records...")
  collection.updateMany(lt("i", 4), inc("i", 100)).printResults()
  collection.find().printResults()  
  
  println("\nDelete one record...")
  collection.deleteOne(equal("i", 6)).printResults()
  collection.find().printResults()
  
  println("\nDelete multiple records...")
  collection.deleteMany(gte("i", 100)).printResults()
  collection.find().printResults()
  
  // Define multiple document insertion, update, deletion and replacement operations to be executed in bulk.
  val documentOperations: List[WriteModel[_ <: Document]] = List(
      InsertOneModel(Document("_id" -> 4)),
      InsertOneModel(Document("_id"-> 5)),
      InsertOneModel(Document("_id" -> 6)),
      InsertOneModel(Document("_id" -> 7)),
      UpdateOneModel(Document("_id" -> 5), set("x", 2)),
      DeleteOneModel(Document("_id" -> 4)),
      ReplaceOneModel(Document("_id" -> 6), Document("_id" -> 6, "x" -> 4))
    )

  println("\nPerform document operations in bulk with order guaranteed...")
  collection.bulkWrite(documentOperations).printResults()
  collection.find().printResults()
  
  // For unordered bulk operation, do this instead.
  // collection.bulkWrite(documentOperations, BulkWriteOptions().ordered(false)).printResults()

  mongoClient.close()
}
