package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import us.dac.Helpers._


object MapReduce extends App {
  
  // Connect to the "MongoScalaDriverDemo" database and make a new collection called "CRUD".
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("MapReduce")
  
//  println("\nRun a command (to get build info) that has no helper in the MongoDB Scala Driver...")
//  val buildInfoCommand = Document("buildInfo" -> 1)
//  database.runCommand(buildInfoCommand).printResults()
  
  mongoClient.close()
}
