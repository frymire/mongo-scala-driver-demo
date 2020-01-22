package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Indexes._

import org.bson.conversions.Bson

import us.dac.Helpers._


object AdminDemo extends App {
  
  // Connect to the "MongoScalaDriverDemo" database and make a new collection called "AdminDemo".
  val mongoClient = MongoClient()

  println("\nList all databases...")
  mongoClient.listDatabaseNames().printResults()
   
  val dummyDB = mongoClient.getDatabase("DummyDB")
  val dummyCollection: MongoCollection[Document] = dummyDB.getCollection("Dummy")

  // Counter-intuitively, the database isn't created yet, because we must first add documents.
  println("\nList all databases before adding documents to the Dummy db...")
  mongoClient.listDatabaseNames().printResults()
  
  // Add a collection and a documents to the dummy database.
  dummyCollection.insertOne(Document("hello" -> "world", "goodbye" -> "cruel world")).results()
  
  println("\nAfter adding a document to the Dummy db, it shows up...")
  mongoClient.listDatabaseNames().printResults()
  
  println("\nAfter dropping the Dummy db...")
  mongoClient.getDatabase("DummyDB").drop().results()
  mongoClient.listDatabaseNames().printResults()
  
  
  // Switch to the main database
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
    
  println("\nCreate and populate a new AdminDemo collection...")
  val collection: MongoCollection[Document] = database.getCollection("AdminDemo")
  val someDocuments = List(
      Document("_id" -> 0, "i" -> 8, "content" -> "textual content"),
      Document("_id" -> 1, "i" -> 4, "content" -> "additional content"),
      Document("_id" -> 2, "i" -> 5, "content" -> "irrelevant content")
    )
  collection.insertMany(someDocuments).results()
  collection.find().printResults()
 
  println("\nCreate an index on the 'i' field of the AdminDemo collection, sorted in ascending order...")
  collection.createIndex(ascending("i")).printResults("Created an index named: ")

  println("\nCreate a text index on the 'content' field of the AdminDemo collection...")
  collection.createIndex(Document("content" -> "text")).printResults("Created an index named: ")
  
  collection.listIndexes().printResults()
  
  println("\nDrop the new index...")
  collection.dropIndex("content_text").printResults()
  collection.listIndexes().printResults()
  
  println("\nList the collections in the MongoScalaDriverDemo db...")
  database.listCollectionNames().printResults()

  println("\nDrop the collection AdminDemo and list collections again...")
  collection.drop().printResults()
  database.listCollectionNames().printResults()
  
  println("\nRun a command (to get build info) that has no helper in the MongoDB Scala Driver...")
  val buildInfoCommand: Bson = Document("buildInfo" -> 1)
  database.runCommand(buildInfoCommand).printResults()
  
  mongoClient.close()
}
