package us.dac.hamming

import java.util.Date 

import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates.{filter, sort, out}
import org.mongodb.scala.model.Filters.{or, equal, in}
import org.mongodb.scala.model.Sorts.{orderBy, ascending}

import us.dac.ObservableHelpers._
import us.dac.BinaryHelpers._


// NOTE: Run HammingDB to populate the "Hamming" collection before running this.
object HammingNearestNeighbors extends App {

  val query42 = Code32(42)
  val queryRadius = 8
  val subcodeRadius = queryRadius / query42.codes.length

  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(Code32.codecRegistry)
  val codeCollection: MongoCollection[Code32] = database.getCollection("Hamming")

  val subcodes = new SubcodeGenerator(8, 4)   // makes an 8-bit subcode generator with a maximum radius of 4 bits  

  val tic = new Date().getTime()
  
  println(s"\nQuery for documents with any subcode within a Hamming distance of $subcodeRadius from the corresponding subcode in 42...")
  println(s"$query42 <-- query")
  val querySubcodes = Code32.codeByteRange map { i => subcodes.withinRadiusXFrom(subcodeRadius, query42.getSubcode(i)) }

//  (0 to 3) foreach { j => querySubcodes(j) foreach { i => println(codeCollection.find( equal(s"code$j", byte2Bson(i)) ).results().length) } }
//  println(s"Duration: ${new Date().getTime() - tic}")
  
  val codeMatchFilters = Code32.codeByteRange map { i => in(s"code$i", querySubcodes(i) map { byte2Bson(_) } :_*) }
  val pipelineAnySubcodeWithinRadius = Seq(
      filter(or(codeMatchFilters:_*)),      
//      sort( orderBy(ascending("code")) ),
      out("Query42Candidates")
    ) 
  val anySubcodeWithinRadius = codeCollection.aggregate(pipelineAnySubcodeWithinRadius).results()
//  anySubcodeWithinRadius foreach { code => 
//    print(s"$code\t")
//    Code32.codeByteRange.reverse foreach { i => print(s"\tcode$i = ${hamming(code.getSubcode(i), query42.getSubcode(i))}") }
//    println(s"\tOverall = ${hamming2(code.code, query42.code)}") 
//  }    
  println(s"Number of documents with any subcode within Hamming radius $subcodeRadius: ${anySubcodeWithinRadius.length}")
  println(s"Elapsed (query): ${new Date().getTime() - tic}")
  
  println(s"\nFilter down to documents with code within Hamming radius $queryRadius of the query 42...")
  // TODO: We need a way here for MongoDB to check the total Hamming distance of the candidates from the query. 
  val queryResult = anySubcodeWithinRadius filter { code => hamming2(code.code, query42.code) <= queryRadius }
//  queryResult foreach println
  println(queryResult.length)
  println(s"Elapsed (filter): ${new Date().getTime() - tic} ms")
    
  mongoClient.close()
}
