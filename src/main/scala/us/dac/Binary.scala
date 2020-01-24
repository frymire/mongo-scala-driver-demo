package us.dac

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.{equal}
import org.mongodb.scala.model.Aggregates.{filter, group, sort, project, out}
import org.mongodb.scala.model.Projections.{fields, include, computed}
import org.mongodb.scala.model.Updates.{combine, set, bitwiseAnd, bitwiseOr, bitwiseXor}

import us.dac.Helpers._
import org.mongodb.scala.model.Projections

// https://docs.mongodb.com/manual/reference/operator/update/bit/
// https://docs.mongodb.com/manual/reference/operator/query-bitwise/
// https://en.wikipedia.org/wiki/Base64

object Binary extends App {
   
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("Binary")
  collection.drop().results()

  println("\nAdd a new set of documents...")
  val documents = List(
      Document("_id" -> 1, "code" -> 13),
      Document("_id" -> 2, "code" -> 3),
      Document("_id" -> 3, "code" -> 1)
    )
  collection.insertMany(documents).printResults()
  collection.find().printResults()

  val andOp = Document("$bit" -> Document("code" -> Document("and" -> 10) ))

  println("\nCopy the codes to new fields to store the results of the bitwise operations...")
  val pipeline1 = Seq(
    project( fields(include("code"), computed("And10", "$code"), computed("Or5", "$code"), computed("Xor5", "$code")) ),
    out("Codes")
  )
  collection.aggregate(pipeline1).printResults()
  
  val outCollection: MongoCollection[Document] = database.getCollection("Codes")  
  println("\nPerform the bitwise operations...")
  val filterQuery = Document()
  val updateQuery = combine(bitwiseAnd("And10", 10), bitwiseOr("Or5", 5), bitwiseXor("Xor5", 5))
  outCollection.updateMany(filterQuery, updateQuery).printResults()
  outCollection.find().printResults()  
  
  mongoClient.close()
}
