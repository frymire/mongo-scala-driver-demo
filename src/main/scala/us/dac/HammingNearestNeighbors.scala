package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonBinary
import org.mongodb.scala.model.Filters.{equal, in}
import org.mongodb.scala.model.Indexes.hashed

import us.dac.Helpers._

object HammingNearestNeighbors extends App {
  
  val codeNumBytes = 4
  val codeNumBits = 8*codeNumBytes
  val codeHighByteIndex = codeNumBytes - 1
  
  /** Returns a byte from a list of positions with bits to be set to 1. */
  def bits2Byte(ones: List[Int]) = ones map { (1 << _) } reduce { _ | _ } toByte;

  /** Returns the byte in the specified position of the provided Int. */
  def getByteFromInt(x: Int, byteIndex: Int): Byte = (x >> 8*byteIndex).toByte
  
  /** Returns a BsonBinary instance populated with a provided Byte. */
  def byte2Bson(b: Byte) = BsonBinary(Array(b))
  
  /** Returns an integer based on a binary string. */
  def binaryString2Int(str: String) = Integer.parseInt(str.replaceAll("\\s", ""), 2)

  /** Returns a string that results from prepending a provided string with 0s to reach a specified length. */
  def pad(str: String, nBits: Int = codeNumBits) = {
    val unpaddedString = str.takeRight(nBits)
    ("0" * (nBits - unpaddedString.length)) + unpaddedString     
  }
  
  /** Returns a string representation of a byte, padded to eight characters. */
  def byte2String(theByte: Byte) = pad(theByte.toBinaryString, 8)
  
  def createDocumentForCode(code: Int) = {
    val subcodes = (0 to codeHighByteIndex) map { i => getByteFromInt(code, i) }    
    Document(
        "code" -> code,  
        "code3" -> byte2Bson(subcodes(3)), 
        "code2" -> byte2Bson(subcodes(2)), 
        "code1" -> byte2Bson(subcodes(1)), 
        "code0" -> byte2Bson(subcodes(0)), 
        "codeString" -> pad(code.toBinaryString),
        "code3String" -> byte2String(subcodes(3)),
        "code2String" -> byte2String(subcodes(2)),
        "code1String" -> byte2String(subcodes(1)),
        "code0String" -> byte2String(subcodes(0))
      )
  }
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("Hamming")
  collection.drop().results()

  println("\nAdd a set of documents with random and specific integer codes...")
  val specificDocuments = List(39, 42) map { i => createDocumentForCode(i) }  
  val randomDocuments = (1 to 1000) map { i => createDocumentForCode(util.Random.nextInt()) }  
  collection.insertMany(specificDocuments ++ randomDocuments).printResults()
  collection.find().limit(10).printResults()

  println("\nCreate hashed indexes on the \'code\' fields...")
  collection.createIndex(hashed("code")).printResults()
  (0 to 3) foreach { i => collection.createIndex(hashed(s"code$i")).printResults() } 
  collection.listIndexes().printResults()

  println("\nReport the operations used to query for \'code\' in [39, 42]...")
  // The Mongo Scala driver doesn't support the explain() method for analyzing queries, so use runCommand()
  val commandJSON = Document("""{ explain: { find: "Hamming", filter: { "code": {"$in": [39, 42] } } } }""")
  database.runCommand(commandJSON).printResults()
  println("\nShow the query results...")
  collection.find(in("code", 39, 42)).limit(10).printResults()
  
  val queryValue = 42
  val queryBytes = (0 to codeHighByteIndex) map { i => byte2Bson(getByteFromInt(queryValue, i)) } 
  
  println("\nQuery for matches of 42 in the most significant byte...")
  collection.find( equal("code3", queryBytes(codeHighByteIndex)) ).limit(10).printResults()
  
  println("\nQuery for matches of 42 in the least significant byte...")
  collection.find( equal("code0", queryBytes(0)) ).limit(10).printResults()

  println("\nQuery for matches of 39 or 42 in the least significant byte...")
  val queryTwoLSBs = List(39, 42) map { i => byte2Bson(getByteFromInt(i, 0)) }
  // Append the splat operator ":_*" to the query List to force Scala to recognize it as varargs.
  collection.find( in("code0", queryTwoLSBs:_*) ).limit(10).printResults()

  println("\nCreate the Byte b01001000 by specifying the bits to be flipped...")
  val ones = List(6, 3) 
  println(byte2String(bits2Byte(ones)))

  println("\nList all bytes with two bits set to 1...")
  val twoBitBytes = (0 to 7).combinations(2) map { c => bits2Byte(c.toList) } 
  twoBitBytes foreach { b => println(byte2String(b)) }
  
  println("\nList all bytes with a Hamming distance of two from the value 255...")
  val twoFrom256 = (0 to 7).combinations(2) map { c => (bits2Byte(c.toList) ^ 255).toByte }
  twoFrom256 foreach { b => println(byte2String(b)) }
  
  mongoClient.close()
}
