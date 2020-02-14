package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonBinary
import org.mongodb.scala.bson.codecs.Macros.{createCodecProvider, createCodecProviderIgnoreNone}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Aggregates.{filter, sort, out}
import org.mongodb.scala.model.Filters.{or, equal, in}
import org.mongodb.scala.model.Indexes.hashed
import org.mongodb.scala.model.Sorts.{orderBy, ascending}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._

import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

import us.dac.ObservableHelpers._
import us.dac.BinaryHelpers._


case class Code(
    code: Int, 
    code3: Array[Byte], 
    code2: Array[Byte], 
    code1: Array[Byte],
    code0: Array[Byte]) {

  val codes = List(code3, code2, code1, code0) map { _(0) }
  
  def getSubcode(position: Int) = byte2Bson(codes(Code.codeHighByte - position))

  override def toString() = {
    val integerCodeStrings = codes map {c => f"${ if (c >= 0) c else (c + 256) }%4d"} mkString " "
    val binaryCodeStrings = codes map { byte2String(_) } mkString " "
    f"$code%11d, [$integerCodeStrings], [$binaryCodeStrings]"
  }
}

object Code {
  
  /*
   * NOTE: The optimum substring length is log-base-2(numDocuments). So, with 2^16 = 65,536 records,
   * you would want substrings of 16 bits. For 64-bit codes, then, you would have 4 substrings.
   */
  val codeNumBytes = 4
  val codeNumBits = 8*codeNumBytes
  val codeHighByte = codeNumBytes - 1
  val codeByteRange = 0 to codeHighByte
  
//  def createDocumentForCode(code: Int) = {
//    val subcodes = codeByteRange map { i => getByteFromInt(code, i) }
//    val codes = codeByteRange.reverse map { i => Document(s"code${i}" -> byte2Bson(subcodes(i))) } reduce { _ ++ _ }
//    val codeStrings = 
//      codeByteRange.reverse map { i => Document(s"code${i}String" -> byte2String(subcodes(i))) } reduce { _ ++ _ }
//    Document("code" -> code) ++ codes ++ Document("codeString" -> pad(code.toBinaryString, codeNumBits)) ++ codeStrings
//  }
  
  /** Returns a new Code instance based on the provided Int value. */
  def apply(code: Int) = { 
    val subcodes = codeByteRange map { i => getByteFromInt(code, i) }
    new Code(
        code, 
        Array(subcodes(3)),
        Array(subcodes(2)),
        Array(subcodes(1)),
        Array(subcodes(0)) 
    )
  }
}

object HammingNearestNeighbors extends App {

  // Specify a codec registry to convert the Code type to and from BSON.
  val codeCodecProvider = createCodecProviderIgnoreNone[Code]()
  val codecRegistry = fromRegistries(fromProviders(codeCodecProvider), DEFAULT_CODEC_REGISTRY)

  // Make two Hamming collection references, one for generic documents and the other specifically for Codes.
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(codecRegistry)
  val documentCollection: MongoCollection[Document] = database.getCollection("Hamming")
  val codeCollection: MongoCollection[Code] = database.getCollection("Hamming")
  codeCollection.drop().results()

  println("\nAdd a set of documents with random and specific integer codes...")
  val specificDocuments = List(32, 39, 42, 44, 104) map { i => Code(i) }  
  val randomDocuments = (1 to 1000) map { i => Code(util.Random.nextInt()) }  
  codeCollection.insertMany(specificDocuments ++ randomDocuments).printResults()

  println("\nList the first 10 documents using the Document JSON converter...")
  documentCollection.find().limit(10).printResults()

  println("\nList the first 10 documents using the Code case class converter...")
  codeCollection.find().limit(10).printResults()

  println("\nCreate hashed indexes on the \'code\' fields...")
  codeCollection.createIndex(hashed("code")).printResults()
  Code.codeByteRange foreach { i => codeCollection.createIndex(hashed(s"code$i")).printResults() } 
  codeCollection.listIndexes().printResults()

  val query42 = Code(42)
  
  println("\nQuery for the first 10 matches of 42 in the least significant byte...")
  codeCollection.find( equal("code0", query42.getSubcode(0)) ).limit(10).printResults()

  println("\nQuery for the first 10 matches of 42 in the most significant byte...")
  codeCollection.find( equal(s"code${Code.codeHighByte}", query42.getSubcode(Code.codeHighByte)) ).limit(10).printResults()
  
  // TODO: Compute and print the Hamming distance for the whole code with these results.
  println("\nQuery for documents at a max Hamming distance of 3 from 42 (at least one of the 4 subcodes must match)...")
  val codeMatches = Code.codeByteRange map { i => equal(s"code$i", query42.getSubcode(i)) }
  val pipelineRadius0 = Seq(
      filter(or(codeMatches:_*)),
      sort(orderBy(ascending("code"))),
      out("HammingOut")
    ) 
  codeCollection.aggregate(pipelineRadius0).printResults()
  
  println("\nSame results when retrieved after saving to a separate collection...")
  val outCollection: MongoCollection[Code] = database.getCollection("HammingOut")
  outCollection.find().printResults()
  
  // Now demonstrate queries for multiple binary values...
  
  println("\nReport the operations used to query for a code of 39 or 42...")
  // The Mongo Scala driver doesn't support the explain() method for analyzing queries, so use runCommand()
  val commandJSON = Document("""{ explain: { find: "Hamming", filter: { "code": {"$in": [39, 42] } } } }""")
  database.runCommand(commandJSON).printResults()
  println("\nShow the query results...")
  codeCollection.find(in("code", 39, 42)).limit(10).printResults()
  
  println("\nQuery for matches of 39 or 42 in the least significant byte...")
  val queryTwoLSBs = List(39, 42) map { x => byte2Bson(getByteFromInt(x, 0)) }
  // Append the splat operator ":_*" to the query List to force Scala to recognize it as varargs.
  codeCollection.find( in("code0", queryTwoLSBs:_*) ).limit(10).printResults()

  println("\nCreate the Byte b00101010 for the decimal value 42 by specifying the bits to be flipped...")
  val byte42 = oneBits2Byte(List(5, 3, 1))
  println(byte2String(byte42))

  println("\nList all bytes with two bits set to 1...")
  val twoBitBytes = (0 to 7).combinations(2).toList map { c => oneBits2Byte(c.toList) } 
  twoBitBytes foreach { b => println(byte2String(b)) }
  
    
  println("\nList all bytes at a Hamming distance of two from the value 255 (b11111111)...")
  twoBitBytes map { b => (b ^ 255).toByte } foreach { b => println(byte2String(b)) }
  
  println("\nList all bytes at a Hamming distance of two from the value 42 (b00101010)...")
  val twoFrom42 = twoBitBytes map { b => (b ^ byte42).toByte }
  twoFrom42 foreach { b => println(byte2String(b)) }
  
  println("\nVerify that each previous value has two flipped bits relative to 42...")
  println(twoFrom42 map { b => popcount((b ^ byte42).toByte) } mkString " ")

  println("\nQuery for documents with code0 at a Hamming distance of two from the value 42 (b00101010)...")
  val pipelineCode0 = Seq(
      filter( in("code0", twoFrom42 map { byte2Bson(_) } :_*) ), 
      sort( orderBy(ascending("code")) )      
    ) 
  val code0Radius2 = codeCollection.aggregate(pipelineCode0).results()
  code0Radius2 foreach { doc => println(s"$doc\tcode0 radius = ${hamming(doc.code0.head, byte42)}") }
  println(s"Number of documents with code0 at radius 2: ${code0Radius2.length}")
  
  println("\nExhaustively verify the total number of documents with code0 at a Hamming radius of two from 42...") 
  val numDocsRadius2From42 = codeCollection.find().results() count { doc => hamming(doc.code0.head, byte42) == 2 }
  println(s"Number of documents with code0 at radius 2: $numDocsRadius2From42")

  mongoClient.close()
}
