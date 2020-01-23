package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import us.dac.Helpers._

object ArrayQueries extends App {

  // Connect to the "MongoScalaDriverDemo" database and make a new collection called "ArrayQueries".
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("ArrayQueries")
  collection.drop().results()
  
  
  println("\nAdd a new set of documents with arrays...")
  val documentsWithArrays = List(      
        Document("""{ item: 'journal', qty: 25, tags: ['blank', 'red'], dim_cm: [ 14, 21 ] }"""),
        Document("""{ item: 'notebook', qty: 50, tags: ['red', 'blank'], dim_cm: [ 14, 21 ] }"""),
        Document("""{ item: 'paper', qty: 100, tags: ['red', 'blank', 'plain'], dim_cm: [ 14, 21 ] }"""),
        Document("""{ item: 'planner', qty: 75, tags: ['blank', 'red'], dim_cm: [ 22.85, 30 ] }"""),
        Document("""{ item: 'postcard', qty: 45, tags: ['blue'], dim_cm: [ 10, 15.25 ] }""")
      )
  collection.insertMany(documentsWithArrays).printResults()
  collection.find().printResults()
  
  println("\nQuery the 'tags' array field for 'red' (non-exclusive)...")
  collection.find(equal("tags", "red")).printResults()

  println("\nQuery the 'tags' array field for the full sequence ['blank', 'red']...")
  collection.find(equal("tags", List("blank", "red"))).printResults()
  // Note the difference in exclusiveness of equal() here for individual elements versus full Lists.

  println("\nQuery the 'tags' array field for both 'red' and 'blank' (non-exclusive) in any order...")
  collection.find(all("tags", "red", "blank")).printResults()

  println("\nQuery the 'tags' array field for either 'blue' or 'plain'...")
  collection.find(in("tags", "blue", "plain")).printResults()
  
  println("\nQuery the 'tags' array field for 'red' and 'blank' and not 'plain'...")
  collection.find(and(all("tags", "red", "blank"), not(equal("tags", "plain")))).printResults()

  println("\nQuery for 'tags' fields with something other than 2 elements...")
  collection.find(not(size("tags", 2))).printResults()

  println("\nQuery the 'dim_cm' field for at least one value greater than 25...")
  collection.find(gt("dim_cm", 25)).printResults()
  
  println("\nQuery the 'dim_cm' field for at least one value greater than 15 and at least one (possibly different) value less than 20...")
  collection.find(and(gt("dim_cm", 15), lt("dim_cm", 20))).printResults()

  println("\nQuery the 'dim_cm' field for at least one single value that is both greater than 13 and less than 15...")
  collection.find(elemMatch("dim_cm", Document("""{ $gt: 13, $lt: 15 }"""))).printResults()
  
  println("\nUse dot notation (zero-based index) to query the 2nd element of 'dim_cm' field for values greater than 25...")
  collection.find(gt("dim_cm.1", 25)).printResults()
  
  mongoClient.close()
}
