package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import us.dac.Helpers._

/*
 * Generally, the Aggregation pipeline is supposed to be faster, but the Map-Reduce pipeline
 * lets you define each step as a Javascript function, providing broader flexibility. This 
 * also makes it easy to emit more than one document for each input document. 
 */
object MapReduce extends App {

  // Connect to the "MongoScalaDriverDemo" database and make a new collection called "CRUD".
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("MapReduce")
  collection.drop().results()

  println("\nAdd a new set of documents...")
  val documents = List(   
        Document("""{ cust_id: "A123", amount: 500, status: "A" }"""),
        Document("""{ cust_id: "A123", amount: 250, status: "A" }"""),
        Document("""{ cust_id: "B212", amount: 200, status: "A" }"""),
        Document("""{ cust_id: "A123", amount: 300, status: "D" }""")
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()


  val mapFunction1 = "function() { emit(this.cust_id, this.amount); };"
  val mapFunction2 = "function() { emit(this.status, this.amount); };"
  val mapFunction3 = "function() { for (var i = 0; i < 3; i++) { emit(this.status, this.amount); } };"
  val reduceFunction = "function(key, values) { return Array.sum(values); };"

  println("\nRun map-reduce to sum the amounts by customer ID...")
  collection.mapReduce(mapFunction1, reduceFunction).printResults()

  println("\nRun map-reduce to sum the amounts by status...")
  collection.mapReduce(mapFunction2, reduceFunction).printResults()

  println("\nUse a map function that emits three copies of each input, then sum amounts by status...")
  collection.mapReduce(mapFunction3, reduceFunction).printResults()

  println("\nIf the driver doesn't support some needed Map-Reduce functionality, use a direct database command.")  
  println("Here, query for input documents with amount < 400, sum the amounts by ID, and subtract 1 from the results...")
  val mapReduceCommand = Document(
        "mapReduce" -> "MapReduce",
        "query" -> Document("""{ amount: { $lt: 400 } }"""),
        "map" -> mapFunction1, 
        "reduce" -> reduceFunction,
        "finalize" -> "function(key, reducedValue) { return reducedValue - 1; };",
        "out" -> "MapReduceOutput"        
      )
  database.runCommand(mapReduceCommand).printResults()
  database.getCollection("MapReduceOutput").find().printResults()
  
  mongoClient.close()
}
