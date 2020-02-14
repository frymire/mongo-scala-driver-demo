package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import us.dac.ObservableHelpers._

object EmbeddedDocuments extends App {

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("EmbeddedDocuments")
  collection.drop().results()
  
  println("\nAdd a new set of documents with embedded documents...")
  val documentsWithEmbeddedDocuments = List(      
        Document("""{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"""),
        Document("""{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"""),
        Document("""{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"""),
        Document("""{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"""),
        Document("""{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }""")
      )
  collection.insertMany(documentsWithEmbeddedDocuments).printResults()
  collection.find().printResults()
  
  println("\nQuery for the embedded document \"{ size: { h:14, w:21, uom:'cm' }\"...")
  collection.find(equal("size", Document("h" -> 8.5, "w" -> 11, "uom" -> "in"))).printResults()

  println("\nQuery for the embedded document \"{size: {w:21, h:14, uom:'cm' }\" with fields in the wrong order (no matches)...")
  collection.find(equal("size", Document("w" -> 11, "h" -> 8.5, "uom" -> "in"))).printResults()
  
  println("\nUse dot notation to query on the nested field \"{ size.uom: 'cm' }\"...")
  collection.find(equal("size.uom", "cm")).printResults()
  
  println("\nQuery for documents with size.h less than 14...")
  collection.find(lt("size.h", 14)).printResults()
  
  mongoClient.close()
}
