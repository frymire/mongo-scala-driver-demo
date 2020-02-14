package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonBinary
import org.mongodb.scala.model.Filters.{lt, gte, bitsAllSet, bitsAllClear, bitsAnySet, bitsAnyClear, and}
import org.mongodb.scala.model.Aggregates.{group, project, out}
import org.mongodb.scala.model.Projections.{fields, include, computed}
import org.mongodb.scala.model.Updates.{combine, bitwiseAnd, bitwiseOr, bitwiseXor}

import us.dac.ObservableHelpers._
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

  println("\nCopy the codes to new fields to store the results of the bitwise operations...")
  val pipeline1 = Seq(
    project( fields(include("code"), computed("And10", "$code"), computed("Or5", "$code"), computed("Xor5", "$code")) ),
    out("Codes")
  )
  collection.aggregate(pipeline1).printResults()
  
  println("\nPerform the bitwise operations...")
  val codesCollection: MongoCollection[Document] = database.getCollection("Codes")  
  val filterQuery = lt("_id", 4)
  val updateQuery = combine(bitwiseAnd("And10", 10), bitwiseOr("Or5", 5), bitwiseXor("Xor5", 5))
  codesCollection.updateMany(filterQuery, updateQuery).printResults()
  codesCollection.find(filterQuery).printResults()  
  
  println("\nCreate a BsonBinary object (binary data is encoded as Base64)...")
  val doc4 = Document("""{ _id: 4, code: BinData(0, "Zg=="), inBinary: "01100110", inDecimal: 102 }""")
  println(doc4)

  def toPaddedBinaryString(b: Byte) = {
    val unpaddedString = b.toBinaryString.takeRight(8)
    ("0" * (8 - unpaddedString.length)) + unpaddedString     
  }
  
  println("\nThe BSON representation of Array(102, 0, 127, 128, 255) maps to values in [-128, 127]...") 
  val bsonBinaryFromIntArray = BsonBinary(Array(102, 0, 127, 128, 255) map { _.toByte })
  val doc5 = Document("_id" -> 5, "code" -> bsonBinaryFromIntArray)
  println(doc5)
  bsonBinaryFromIntArray.getData map { b => toPaddedBinaryString(b) } foreach println
  
  println(s"\nThe BSON representation of \'Man is\' matches ASCII codes, since they are all in [0, 127]...")
  val bsonBinaryFromText = BsonBinary("Man is".getBytes)  
  val doc6 = Document("_id" -> 6, "code" -> bsonBinaryFromText) 
  println(doc6)
  bsonBinaryFromText.getData map { b => toPaddedBinaryString(b) } foreach println
  
  println("\nIn query results, binary fields are printed in Base64 encoding...")
  collection.insertMany(List(doc4, doc5, doc6)).printResults()
  collection.find(gte("_id", 4)).printResults()
  
  println("\nQuery for codes that match the binary mask \'01100110\' (102 in decimal, Zg== in Base64)...")
  def binaryString2Int(in: String) = Integer.parseInt(in.replaceAll("\\s", ""), 2)
  collection.find(bitsAllSet("code", binaryString2Int("01100110"))).printResults()

  println("\nRepeat using the decimal expression...")
  collection.find(bitsAllSet("code", 102)).printResults()

  println("\nRepeat using the Base64 expression...")
  collection.find(Document("""{ code: { $bitsAllSet: BinData(0, "Zg==") } } }""")).printResults()

  println("\nThe relaxed binary mask \'00000110\' still works...")
  collection.find(bitsAllSet("code", binaryString2Int("01100110"))).printResults()
    
  println("\nChecking the same mask \'01110110\' for any set bits matches more records...")
  collection.find(bitsAnySet("code", binaryString2Int("00000110"))).printResults()

  println("\nThe more stringent binary mask \'01110110\' doesn't match anything...")
  collection.find(bitsAllSet("code", binaryString2Int("01110110"))).printResults()

  println("\nCheck the opposite mask \'10011001\' for clear bits...")
  collection.find(bitsAllClear("code", binaryString2Int("10011001"))).printResults()

  mongoClient.close()
}
