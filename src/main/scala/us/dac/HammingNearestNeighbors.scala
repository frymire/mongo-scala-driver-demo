package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonBinary
import org.mongodb.scala.bson.codecs.Macros.{createCodecProvider, createCodecProviderIgnoreNone}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Aggregates.{filter, sort, out}
import org.mongodb.scala.model.Filters.{or, equal, in}
import org.mongodb.scala.model.Indexes.hashed
import org.mongodb.scala.model.Sorts.{orderBy, ascending}

import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

import us.dac.ObservableHelpers._
import us.dac.BinaryHelpers._


/*
 * NOTE: The optimum substring length is log-base-2(numDocuments). So, with 2^16 = 65,536 records,
 * you would want substrings of 16 bits. For 64-bit codes, then, you would have 4 substrings.
 */

abstract class Code {
  
  val code: Int
  val codes: List[Byte]
  val codeHighByte: Int

  def getSubcode(position: Int) = codes(codeHighByte - position)
  def getSubcodeBson(position: Int) = byte2Bson(getSubcode(position))  

  override def toString() = {
    val integerCodeStrings = codes map {c => f"${ if (c >= 0) c else (c + 256) }%4d"} mkString " "
    val binaryCodeStrings = codes map { byte2String(_) } mkString " "
    f"$code%11d, [$integerCodeStrings], [$binaryCodeStrings]"
  }
}

/** 32 bit code */
case class Code32(
    val code: Int, 
    code3: Array[Byte], 
    code2: Array[Byte], 
    code1: Array[Byte],
    code0: Array[Byte]) extends Code {

  val codes = List(code3, code2, code1, code0) map { _(0) }
  val codeHighByte = Code32.codeHighByte
}

object Code32 {
  
  val codeNumBytes = 4
  val codeHighByte = codeNumBytes - 1
  val codeByteRange = 0 to codeHighByte
  
  /** Returns a new Code instance based on the provided Int value. */
  def apply(code: Int) = { 
    val subcodes = codeByteRange map { i => getByteFromInt(code, i) }
    new Code32(
        code, 
        Array(subcodes(3)),
        Array(subcodes(2)),
        Array(subcodes(1)),
        Array(subcodes(0)) 
    )
  }
  
  // Specify a codec registry to convert the Code type to and from BSON.
  val codeCodecProvider = createCodecProviderIgnoreNone[Code32]()
  val codecRegistry = fromRegistries(fromProviders(codeCodecProvider), DEFAULT_CODEC_REGISTRY)  
}

object HammingNearestNeighbors extends App {

  // Make two Hamming collection references, one for generic documents and the other specifically for Codes.
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(Code32.codecRegistry)
  val documentCollection: MongoCollection[Document] = database.getCollection("Hamming")
  val codeCollection: MongoCollection[Code32] = database.getCollection("Hamming")
  codeCollection.drop().results()

  println("\nAdd a set of documents with random and specific integer codes...")
  val specificDocuments = List(32, 39, 42, 44, 104, 298) map { i => Code32(i) }  
  val randomDocuments = (1 to 1000) map { i => Code32(util.Random.nextInt()) }  
  codeCollection.insertMany(specificDocuments ++ randomDocuments).printResults()

  println("\nList the first 10 documents using the Document JSON converter...")
  documentCollection.find().limit(10).printResults()

  println("\nList the first 10 documents using the Code case class converter...")
  codeCollection.find().limit(10).printResults()

  println("\nCreate hashed indexes on the \'code\' fields...")
  codeCollection.createIndex(hashed("code")).printResults()
  Code32.codeByteRange foreach { i => codeCollection.createIndex(hashed(s"code$i")).printResults() } 
  codeCollection.listIndexes().printResults()

  println("\nCreate the Byte b00101010 for the decimal value 42 by specifying the bits to be flipped...")
  val query42 = Code32(42)
  
  println("\nQuery for the first 10 matches of 42 in the least significant byte...")
  codeCollection.find( equal("code0", query42.getSubcodeBson(0)) ).limit(10).printResults()

  println("\nQuery for the first 10 matches of 42 in the most significant byte...")
  codeCollection.find( equal(s"code${Code32.codeHighByte}", query42.getSubcodeBson(Code32.codeHighByte)) ).limit(10).printResults()
  
  println("\nQuery for candidate documents at a max Hamming distance of 3 from 42 (at least one of the 4 subcodes must match)...")
  println(s"$query42 <-- query")
  val codeMatches = Code32.codeByteRange map { i => equal(s"code$i", query42.getSubcodeBson(i)) }
  val pipelineRadius0 = Seq(
      filter(or(codeMatches:_*)),
      sort(orderBy(ascending("code"))),
      out("Query42Candidates")
    ) 
  codeCollection.aggregate(pipelineRadius0).results() foreach { code => 
    print(s"$code\t")
    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
    println(s"\tOverall = ${hamming2(code.code, 42)}") 
  }
    
  println("\nFilter down to codes within a distance of 3...")
  val queryResult = codeCollection.aggregate(pipelineRadius0).results() filter { code => hamming2(code.code, 42) <= 3 }
  queryResult foreach { code =>
    print(s"$code\t")
    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
    println(s"\tOverall = ${hamming2(code.code, 42)}") 
  }
  
  // TODO: We need a way here for MongoDB to check the total Hamming distance of the candidates from the query. 
//  println("\nSame results when retrieved after saving to a separate collection...")
//  val outCollection: MongoCollection[Code32] = database.getCollection("Query42Candidates")
//  outCollection.find().printResults()

  
  // Now demonstrate queries for multiple binary values...
  
  println("\nReport the operations used to query for a code of 39 or 42 to verify that MongoDB is using the indexes...")
  // The Mongo Scala driver doesn't support the explain() method for analyzing queries, so use runCommand()
  val commandJSON = Document("""{ explain: { find: "Hamming", filter: { "code": {"$in": [39, 42] } } } }""")
  database.runCommand(commandJSON).printResults()
  codeCollection.find(in("code", 39, 42)).limit(10).printResults()
  
  println("\nQuery for matches of 39 or 42 in the least significant byte...")
  val query39Or42 = List(39, 42) map { x => byte2Bson(getByteFromInt(x, 0)) }
  // Append the splat operator ":_*" to the query List to force Scala to recognize it as varargs.
  codeCollection.find( in("code0", query39Or42:_*) ).limit(10).printResults()

    
  println("\nList all bytes with one bit set to 1...")
  val oneBitBytes = (0 to 7).combinations(1).toList map { c => oneBits2Byte(c.toList) } 
  oneBitBytes foreach { b => println(byte2String(b)) }

  println("\nList all bytes with two bits set to 1...")
  val twoBitBytes = (0 to 7).combinations(2).toList map { c => oneBits2Byte(c.toList) } 
  twoBitBytes foreach { b => println(byte2String(b)) }
    
  println("\nList all bytes at a Hamming distance of two from the value 255 (b11111111)...")
  twoBitBytes map { b => (b ^ 255).toByte } foreach { b => println(byte2String(b)) }
  
  println("\nList all bytes at a Hamming distance of two from the value 42 (b00101010)...")
  val queryValidSubcodes = Code32.codeByteRange map { i => twoBitBytes map { b => (b ^ query42.getSubcode(i)).toByte } }
  queryValidSubcodes(0) foreach { b => println(byte2String(b)) }
  
  println("\nVerify that each previous value has two flipped bits relative to 42...")
  println(queryValidSubcodes(0) map { b => popcount((b ^ query42.getSubcode(0)).toByte) } mkString " ")

  println("\nQuery for documents with any code at a Hamming distance of two from the value 42...")
  println(s"$query42 <-- query")
  val pipelineAnyRadius2 = Seq(
      filter( or(
          in("code3", queryValidSubcodes(3) map { byte2Bson(_) } :_*),
          in("code2", queryValidSubcodes(2) map { byte2Bson(_) } :_*),
          in("code1", queryValidSubcodes(1) map { byte2Bson(_) } :_*),
          in("code0", queryValidSubcodes(0) map { byte2Bson(_) } :_*) 
        ) ), 
      sort( orderBy(ascending("code")) ),
      out("Test")
    ) 
  val anyRadius2 = codeCollection.aggregate(pipelineAnyRadius2).results()
  anyRadius2 foreach { code => 
    print(s"$code\t")
    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
    println(s"\tOverall = ${hamming2(code.code, 42)}") 
  }  
  println(s"Number of documents with any code at radius 2: ${anyRadius2.length}")
  
  println("\nExhaustively verify the total number of documents with any code at a Hamming distance of two from 42...") 
  val numDocsRadius2From42All = codeCollection.find().results() count { code =>
    Code32.codeByteRange map { i => hamming(code.getSubcode(i), query42.getSubcode(i)) == 2 } reduce {_ || _}
  }
  println(s"Number of documents with any code at radius 2 (exhausive): $numDocsRadius2From42All")
  
  mongoClient.close()
}
