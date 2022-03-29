package us.dac.hamming

import java.util.Date 

import org.mongodb.scala._

import us.dac.ObservableHelpers._
import us.dac.BinaryHelpers._

// NOTE: Run HammingDB to populate the "Hamming" collection before running this.
object HammingNearestNeighborsExhaustive extends App {
  
  val query42 = Code32(42)
  val queryRadius = 1
  val subcodeRadius = queryRadius / query42.codes.length
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(Code32.codecRegistry)
  val codeCollection: MongoCollection[Code32] = database.getCollection("Hamming")
  
//  println(s"\nExhaustively verify the total number of documents with any subcode within a Hamming radius of $subcodeRadius from 42...") 
//  val numDocsAnySubcodeWithinRadius = codeCollection.find().results() count { code =>
//    Code32.codeByteRange map { i => hamming(code.getSubcode(i), query42.getSubcode(i)) <= subcodeRadius } reduce { _ || _ }
//  }
//  println(s"\nNumber of documents with any code within radius subcodeRadius (exhausive): $numDocsAnySubcodeWithinRadius") 
  

  println(s"\nExhaustively verify the total number of documents with a code within a Hamming radius $queryRadius of the query 42...")
  val tic = new Date().getTime()
  val queryResult = codeCollection.find().results() filter { code => hamming2(code.code, query42.code) <= queryRadius }
//  queryResult2Exhausive foreach println
  println(s"Number of documents: ${queryResult.length}")
  println(s"Elapsed: ${new Date().getTime() - tic} ms")
  
  mongoClient.close()
}
