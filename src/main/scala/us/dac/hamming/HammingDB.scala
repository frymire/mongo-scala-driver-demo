package us.dac.hamming

import org.mongodb.scala._
import org.mongodb.scala.model.Indexes.hashed
import us.dac.ObservableHelpers._

/**
 * Populates a database with random codes to test the Hamming nearest neighbor algorithm.
 * 
 * Run db.collection.totalIndexSize() to check the size (in bytes) of the indexes created.
 */
object HammingDB extends App {
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(Code32.codecRegistry)
  val codeCollection: MongoCollection[Code32] = database.getCollection("Hamming")
  codeCollection.drop().results()  

  println("\nAdd a set of documents with random and specific integer codes...")
  val specificDocuments = List(32, 39, 42, 44, 104, 298) map { i => Code32(i) }
  codeCollection.insertMany(specificDocuments).printResults()
  (1 to 100) foreach { i =>         
    print(s"Adding batch #$i of 1 million documents...")
    val millionDocuments = (1 to 1000000) map { i => Code32(util.Random.nextInt()) }
    codeCollection.insertMany(millionDocuments).printResults()
  }
//  val remainderDocuments = (1 to 10000) map { i => Code32(util.Random.nextInt()) }  
//  codeCollection.insertMany(specificDocuments ++ remainderDocuments).printResults()

  println("\nCreate hashed indexes on the \'code\' fields...")
  codeCollection.createIndex(hashed("code")).printResults()
  Code32.codeByteRange foreach { i => codeCollection.createIndex(hashed(s"code$i")).printResults() } 
  codeCollection.listIndexes().printResults()
  
  println("\nRun a command (to get build info) that has no helper in the MongoDB Scala Driver...")
  val buildInfoCommand = Document("buildInfo" -> 1)
  database.runCommand(buildInfoCommand).printResults()
  
  mongoClient.close()    
}
