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
object DemoMongoConnection extends App {
  
  // Connect to the "mydb" database and make a new collection called "test".
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("mydb")
  val collection: MongoCollection[Document] = database.getCollection("test")
  collection.drop().results()
  
  // Add a document.
  val doc = 
    Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database", "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
  collection.insertOne(doc).results()
  collection.find.first().printResults()
  println(s"Number of documents (should be 1): ${collection.countDocuments().headResult()}")
  
  println("\nInsert 10 new documents, but without incurring network I/O...")
  val documents = (1 to 10) map { i => Document("i" -> i) }
  val insertObservable = collection.insertMany(documents)
  println(s"Number of documents after inserting 10 (still 1, since no network I/O): ${collection.countDocuments().headResult()}")
  
  println("\nForce it to execute by using results() from the Helpers trait...")
  insertObservable.results()
  println(s"Number of documents (should be 101):  ${collection.countDocuments().headResult()}")
  
  println("\nPrint all of the results...")
  collection.find().printResults()
  
  println("\nQuery for a specific document...")
  collection.find(equal("i", 7)).printResults()
  
  println("\nQuery for a range of documents...")
  val rangeQuery = and(gt("i", 4), lte("i", 7))
  collection.find(rangeQuery).printResults()  
  
  println("\nSort query results in descending order...")
  collection.find(rangeQuery).sort(descending("i")).printResults()  
  
  println("\nExclude the '_id' field...")
  collection.find(rangeQuery).projection(excludeId()).printResults()
  
  println("\nProject down to the '_id' field specifically. This implicitly excludes other unnamed fields...")
  collection.find(rangeQuery).projection(include("_id")).printResults()

  println("\nAggregate to make a field that is 10*i, for records with i > 7...")
  val pipeline1 = Seq(
      filter(gt("i", 7)),
      project( Document("""{ITimes10: {$multiply: ["$i", 10]}}""") )
  ) 
  collection.aggregate(pipeline1).printResults()
  
  println("\nSum the values of the i field...")
  // To use accumulators over the whole document set, group with a groupBy parameter of null.
  val pipeline2 = List(group(null, sum("total", "$i"), avg("mean", "$i")))
  collection.aggregate(pipeline2).printResults()
  
  println("\nUpdate one record...")
  collection.updateOne(equal("i", 10), set("i", 110)).printHeadResult("Update Result: ")
  collection.find().printResults()
  
  println("\nUpdate multiple records...")
  collection.updateMany(lt("i", 4), inc("i", 100)).printHeadResult("Update Result: ")
  collection.find().printResults()  
  
  println("\nDelete one record...")
  collection.deleteOne(equal("i", 6)).printHeadResult("Update Result: ")
  collection.find().printResults()
  
  println("\nDelete multiple records...")
  collection.deleteMany(gte("i", 100)).printHeadResult("Update Result: ")
  collection.find().printResults()
  
  // Define multiple documents to be written in bulk.
  val writes: List[WriteModel[_ <: Document]] = List(
      InsertOneModel(Document("_id" -> 4)),
      InsertOneModel(Document("_id"-> 5)),
      InsertOneModel(Document("_id" -> 6)),
      InsertOneModel(Document("_id" -> 7)),
      UpdateOneModel(Document("_id" -> 5), set("x", 2)),
      DeleteOneModel(Document("_id" -> 4)),
      ReplaceOneModel(Document("_id" -> 6), Document("_id" -> 6, "x" -> 4))
    )

  println("\nWrite documents in bulk with order guaranteed...")
  collection.bulkWrite(writes).printResults()
//  collection.bulkWrite(writes, BulkWriteOptions().ordered(false)).printResults() // Do this for unordered bulk operation
  collection.find().printResults()
  
  mongoClient.close()
}
