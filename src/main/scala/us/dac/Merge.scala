package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.gt
import org.mongodb.scala.model.Aggregates.{project, filter}
import org.mongodb.scala.model.Projections.{fields, include}

import us.dac.Helpers._


object Merge extends App {

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")

  val source: MongoCollection[Document] = database.getCollection("MergeSource")
  val out: MongoCollection[Document] = database.getCollection("MergeOut")
  source.drop().results()
  out.drop().results()

  println("\nAdd a new set of documents...")
  val documents = List(   
        Document("""{ _id: 1, status: "A" }"""),
        Document("""{ _id: 2, status: "A" }"""),
        Document("""{ _id: 3, status: "A" }"""),
        Document("""{ _id: 4, status: "B" }"""),
        Document("""{ _id: 5, status: "B" }""")
      )
  source.insertMany(documents).printResults()
  source.find().printResults()

  println("\nCopy documents with _id > 2 to the MergeOut collection, projecting down to the _id only...")
  val gt2 = Seq(
      project(fields(include("_id"))),
      filter(gt("_id", 2)),
      Document("""{ $merge: { into: "MergeOut", on: "_id" } }""")      
    ) 
  source.aggregate(gt2).printResults()  
  out.find().printResults()
  
  println("\nCopy documents with _id > 3, keeping the existing documents when matches are found...")
  val gt3keepExisting = Seq(
      filter(gt("_id", 3)),
      Document("""{ $merge: { into: "MergeOut", on: "_id", whenMatched: "keepExisting" } }""")      
    ) 
  source.aggregate(gt3keepExisting).printResults()  
  out.find().printResults()
  
  println("\nCopy documents with _id > 3, replace with the new documents when matches are found...")
  val gt3Replace = Seq(
      filter(gt("_id", 3)),
      Document("""{ $merge: { into: "MergeOut", on: "_id", whenMatched: "replace" } }""")      
    ) 
  source.aggregate(gt3Replace).printResults()  
  out.find().printResults()
  
  // NOTE: The default behavior of "whenMatched" in $merge command is to merge fields of the matched documents.
  // Alternatively, setting the parameter to "fail" will generate an error if a document matches.
  
  mongoClient.close()
}
