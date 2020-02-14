package us.dac

import org.mongodb.scala._

import us.dac.ObservableHelpers._

object Substrings extends App {
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("Substrings")
  collection.drop().results()

  println("\nAdd a new set of documents...")
  val documents = List(   
        Document("""{ "_id": 1, "item": "ABC1", quarter: "13Q1", "description": "product 1", "code": 8 }"""),
        Document("""{ "_id": 2, "item": "ABC2", quarter: "13Q4", "description": "product 2" , "code": 16 }"""),
        Document("""{ "_id": 3, "item": "XYZ1", quarter: "14Q2", "description": null, "code": 24 }""")
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()

  println("\n...")
  val pipeline1 = Seq(
        Document("""{$project: { 
          item: 1, 
          yearSubstring: {$substrBytes: ["$quarter", 0, 2]},
          quarterSubstring: {$substrBytes: ["$quarter", 2, {$subtract: [ { $strLenBytes: "$quarter"}, 2 ] } ]}
        }}""")
      )
  collection.aggregate(pipeline1).printResults()
  
  mongoClient.close()  
}
