package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._

import us.dac.ObservableHelpers._

// Before running, call "mongod --dbpath C:\Users\Mark.E.Frymire\Documents\MongoDB" or equivalent.
object Projection extends App {
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("CRUD")
  collection.drop().results()
  
  println("\nAdd a new set of documents with embedded documents...")
  val documents = List(      
        Document("""{ item: 'journal', status: 'A', size: { h: 14, w: 21, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 5 }]}"""),
        Document("""{ item: 'notebook', status: 'A',  size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'C', qty: 5}]}"""),
        Document("""{ item: 'paper', status: 'D', size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'A', qty: 60 }]}"""),
        Document("""{ item: 'planner', status: 'D', size: { h: 22.85, w: 30, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 40}]}"""),
        Document("""{ item: 'postcard', status: 'A', size: { h: 10, w: 15.25, uom: 'cm' }, instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }""")
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()
  
  println("\nExclude the _id field...")
  collection.find().projection(excludeId()).printResults()
  
  println("\nProject down to the _id field specifically. This implicitly excludes other unnamed fields...")
  collection.find().projection(include("_id")).printResults()
  
  println("\nProject down to the item and status fields, implicitly including the _id field and excluding all others...")
  collection.find(equal("status", "A")).projection(include("item", "status")).printResults()
  
  println("\nProject down to the item and status fields, exclude the _id field explicitly and all others implicitly...")
  collection.find(equal("status", "A")).projection(fields(include("item", "status"), excludeId())).printResults()

  println("\nExclude the item and status fields, including all others implicitly...")
  collection.find(equal("status", "A")).projection(exclude("item", "status")).printResults()

  println("\nExclude the _id, item, and status fields, including all others implicitly...")
  collection.find(equal("status", "A")).projection(exclude("_id", "item", "status")).printResults()
  
  println("\nProject down to the item, status, and embedded size.uom fields, implicitly including _id...")
  collection.find().projection(include("item", "status", "size.uom")).printResults()
  
  println("\nExclude the embedded size.uom and instock.qty fields...")
  collection.find().projection(exclude("size.uom", "instock.qty")).printResults()
  
  println("\nProject the second element of the instock array...")
  collection.find().projection(slice("instock", 1, 1)).printResults()

  println("\nProject to the last element of the instock array...")
  collection.find().projection(slice("instock", -1)).printResults()
  
  println("\nNote that you cannot use array indices to project to specific array elements...")
  collection.find().projection(include("instock.0")).printResults()
  
  mongoClient.close()
}
