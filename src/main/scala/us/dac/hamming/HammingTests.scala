package us.dac.hamming

import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates.{filter, sort, out}
import org.mongodb.scala.model.Filters.{or, equal, in}
import org.mongodb.scala.model.Sorts.{orderBy, ascending}

import us.dac.ObservableHelpers._
import us.dac.BinaryHelpers._


// NOTE: Run HammingDB to populate the "Hamming" collection before running this.
object HammingTests extends App {
  
  // Make two Hamming collection references, one for generic documents and the other specifically for Codes.
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(Code32.codecRegistry)
  val documentCollection: MongoCollection[Document] = database.getCollection("Hamming")
  val codeCollection: MongoCollection[Code32] = database.getCollection("Hamming")
  
  println("\nList the first 10 documents using the Document JSON converter...")
  documentCollection.find().limit(10).printResults()

  println("\nList the first 10 documents using the Code case class converter...")
  codeCollection.find().limit(10).printResults()

  // Define a query as the 32-bit code for the value 42.
  val query42 = Code32(42)
  
  println("\nQuery for the first 10 matches of 42 in the least significant byte...")
  codeCollection.find( equal("code0", query42.getSubcodeBson(0)) ).limit(10).printResults()

  println("\nQuery for the first 10 matches of 42 in the most significant byte...")
  codeCollection.find( equal(s"code${Code32.codeHighByte}", query42.getSubcodeBson(Code32.codeHighByte)) ).limit(10).printResults()
  
  println("\nQuery for candidate documents (limit 100) within a Hamming radius of 3 from 42 (at least one of the 4 subcodes must match)...")
  println(s"$query42 <-- query")
  val codeMatches = Code32.codeByteRange map { i => equal(s"code$i", query42.getSubcodeBson(i)) }
  val pipelineRadius0 = Seq(
      filter(or(codeMatches:_*)),
      sort(orderBy(ascending("code"))),
      out("Query42Candidates_Radius0")
    ) 
  val radius0Candidates = codeCollection.aggregate(pipelineRadius0).results() 
  radius0Candidates.take(100) foreach { code => 
    print(s"$code\t")
    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
    println(s"\tOverall = ${hamming2(code.code, query42.code)}") 
  }
    
  println("\nFilter down to codes within a Hamming radius of 3...")
  val radius0Results = radius0Candidates filter { code => hamming2(code.code, query42.code) <= 3 }
  radius0Results foreach { code =>
    print(s"$code\t")
    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
    println(s"\tOverall = ${hamming2(code.code, query42.code)}") 
  }
  
  
  // Now demonstrate queries for multiple binary values...
  
  println("\nTo verify that MongoDB is using the indexes, report the operations used to query for a code of 39 or 42...")
  // The Mongo Scala driver doesn't support the explain() method for analyzing queries, so use runCommand()
  val commandJSON = Document("""{ explain: { find: "Hamming", filter: { "code": {"$in": [39, 42] } } } }""")
  database.runCommand(commandJSON).printResults()
  codeCollection.find(in("code", 39, 42)).limit(10).printResults()
  
  println("\nQuery for matches of 39 or 42 in the least significant byte...")
  val query39Or42 = List(39, 42) map { x => byte2Bson(getByteFromInt(x, 0)) }
  // Append the splat operator ":_*" to the query List to force Scala to recognize it as varargs.
  codeCollection.find( in("code0", query39Or42:_*) ).limit(10).printResults()
  
  // Make an 8-bit subcode generator with a maximum radius of 4 bits 
  val subcodes = new SubcodeGenerator(8, 4)  
  subcodes.printUpToRadius(2)
  
  println("\nList all bytes at a Hamming distance of two from the value 255 (b11111111)...")
  subcodes.atDistanceXFrom(2, 255.toByte) foreach { b => println(byte2String(b)) }
  
  val queryRadius = 2
  println(s"\nList all bytes within a Hamming radius of $queryRadius from the value 42 (b00101010)...")
  val querySubcodes = Code32.codeByteRange map { i => subcodes.withinRadiusXFrom(queryRadius, query42.getSubcode(i)) }
  querySubcodes(0) foreach { b => println(byte2String(b)) }
  
  println(s"\nVerify that each previous value has $queryRadius flipped bits relative to 42...")
  println(querySubcodes(0) map { b => popcount((b ^ query42.getSubcode(0)).toByte) } mkString " ")  
}
