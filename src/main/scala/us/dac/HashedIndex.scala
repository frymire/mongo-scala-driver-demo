package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Indexes.hashed

import us.dac.Helpers._

object HashedIndex extends App {
   
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("HashedIndex")
  collection.drop().results()

  def pad(str: String, nBits: Int = 16) = {
    val unpaddedString = str.takeRight(nBits)
    ("0" * (nBits - unpaddedString.length)) + unpaddedString     
  }
  
  println("\nAdd a set of documents with random integer codes...")
  val documents = (1 to 1000000) map { i => 
    val code = util.Random.nextInt(256*256)
    Document("code" -> code, "codeString" -> pad(code.toBinaryString)) 
  }
  
  collection.insertMany(documents :+ Document("code" -> 42, "codeString" -> pad(42.toBinaryString))).printResults()
  collection.find().limit(10).printResults()

  println("\nReport the operations used to query for \'code\' = 42...")
  // The Mongo Scala driver doesn't support the explain() method for analyzing queries, so use runCommand()
  val command = Document("explain" -> Document("find" -> "HashedIndex", "filter" -> Document("code" -> 42)))
  database.runCommand(command).printResults()
    
  println("\nCreated a hashed index on the \'code\' field...")
  collection.createIndex(hashed("code")).printResults()
  
  println("\nRerun the query (~100x faster)...")
  database.runCommand(command).printResults()
  
  println("\nShow the query results...")
  collection.find(equal("code", 42)).limit(10).printResults()
  
  mongoClient.close()
}
