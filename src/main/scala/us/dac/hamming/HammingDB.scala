package us.dac.hamming

import org.mongodb.scala._
import org.mongodb.scala.model.Indexes.hashed
import us.dac.ObservableHelpers._


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
//  val remainderDocuments = (1 to 1000000) map { i => Code32(util.Random.nextInt()) }  
//  codeCollection.insertMany(specificDocuments ++ remainderDocuments).printResults()

  println("\nCreate hashed indexes on the \'code\' fields...")
  codeCollection.createIndex(hashed("code")).printResults()
  Code32.codeByteRange foreach { i => codeCollection.createIndex(hashed(s"code$i")).printResults() } 
  codeCollection.listIndexes().printResults()
  
  mongoClient.close()    
}
