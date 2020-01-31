package us.dac

import java.util.Date
import java.text.SimpleDateFormat

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.{exists, equal, gt}
import org.mongodb.scala.model.Sorts.{orderBy, descending}
import org.mongodb.scala.model.Projections.{fields, include, excludeId}
import org.mongodb.scala.model.Aggregates.{filter, group, sort, project, out}
import org.mongodb.scala.model.Updates.{combine, set, currentDate, currentTimestamp}

import us.dac.Helpers._


object Dates extends App {

  // Use Java utilities to define date formats.
  val format1 = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss zzz")
  val format2 = new SimpleDateFormat("MM/dd/yyyy hh:mm aa zzz")

  val kurt = format1.parse("8/1/1978 17:00:00 CST")
  println(s"Kurt's birthday as milliseconds after Unix epoch: ${kurt.getTime()}")
  println(s"Kurt's birthday as a time instant: ${kurt.toInstant()}")
  println(s"Kurt's birthday as a string: $kurt")
  
  val mongoClient = MongoClient()
  val database = mongoClient.getDatabase("MongoScalaDriverDemo")
  val collection: MongoCollection[Document] = database.getCollection("Dates")
  collection.drop().results()

  println("\nAdd a new set of documents with dates...")
  // NOTE: ISODate() is a wrapper that instantiates dates as long integers from ISO formatted date strings.  
  val documents = List(   
        Document("""{ name: "Unix Epoch", date: { "$date": "1970-01-01T00:00:00.000Z" } }"""),
        Document("""{ name: "1 second before Unix Epoch", date: ISODate("1969-12-31T23:59:59.000Z") } }"""),
        Document("""{ name: "1 second after Unix Epoch", date: ISODate("1970-01-01T00:00:01.000Z") }"""),
        Document("""{ name: "1 second after Unix Epoch in EST", date: ISODate("1970-01-01T00:00:01.000-0500") }"""),
        Document("""{ name: "1 second after Unix Epoch as long int", date: {$date: 1000} }"""),
        Document("""{ name: "1 second after Unix Epoch as JavaScript date", date: new Date(1000) }"""),
        Document("name" -> "Mark", "date" -> format2.parse("1/24/1975 2:10 am CST")),
        Document("name" -> "Kurt", "date" -> kurt),
        Document("""{ name: "Anna", date: ISODate("1982-12-18") }"""),
        Document("""{ name: "Anna as date parts", "dateParts" : {'year' : 1982, 'month' : 12, 'day': 18}}"""),
        Document("""{name: "Edward in UTC", date: ISODate("2018-08-12T00:05:00.000Z") }"""),
        Document("""{ name: "Edward in EDT", date: new Date("Sat Aug 11 2018 20:05:00 EDT") }"""),
        Document("""{ name: "Edward in EST (wrong timezone)", date: new Date("Sat Aug 11 2018 20:05:00 EST") }"""),
        Document("""{ name: "Null", date: null }"""),
        Document("name" -> "Just Born", "date" -> new Date())
      )
  collection.insertMany(documents).printResults()
  collection.find().printResults()

  println("\nAdd the current date and timestamp to null date fields...")
  collection.updateMany(equal("date", null), combine(currentDate("date"), currentTimestamp("updated"))).printResults()
  collection.find().printResults()

  println("\nPrint the dates in a various formats...")
  val dateFormattingPipeline = Seq( project( fields(
          excludeId(),
          include("name", "date"),
          Document("""{dateInstant: { $dateToString: { date: "$date" } } }"""),
          Document("""{dateUTC: { $dateToString: { date: "$date", format: "%m/%d/%Y %H:%M:%S" } } }"""),
          Document("""{dateEST: { $dateToString: { date: "$date", format: "%m/%d/%Y %H:%M:%S", timezone: "EST" } } }"""),
          Document("""{dateAmericaNY: { $dateToString: { date: "$date", format: "%m/%d/%Y %H:%M:%S", timezone: "America/New_York" } } }"""),
          Document("""{day: { $dayOfWeek: "$date" } }"""),
      ) ) ) 
  collection.aggregate(dateFormattingPipeline).printResults()  
  
  println("\nList documents with dates after 1 Jan 1980, defined using Mongo ISODate...")
  collection.find(Document("""{ date: { $gt: ISODate("1980-1-1") } }""")).printResults()

  println("\nList documents with dates after 1 Jan 1980, defined java.text.SimpleDateFormat...")
  collection.find(gt("date", format1.parse("1/1/1980 00:00:00 UTC"))).printResults()  

  println("\nMake a collection with documents that define date components separately...")
  val fromPartsDocuments = List(   
      Document("""{ name: "Mark", "year" : 1975, "month" : 1, "day": 24 }"""),
      Document("""{ name: "Anna", "year" : 1982, "month" : 12, "day": 18 }"""),
      Document("""{ name: "Edward", "year" : 2018, "month" : 8, "day": 11 }"""),
      )
  val fromPartsCollection: MongoCollection[Document] = database.getCollection("DatesFromParts")
  fromPartsCollection.drop().results()
  fromPartsCollection.insertMany(fromPartsDocuments).results()
  fromPartsCollection.find().printResults()

  println("\nCompute dates from parts and list the documents in reverse order...")
  val fromPartsPipeline = Seq(
      project(fields(
          include("name", "dateParts"), 
          Document("date" -> Document("""{ $dateFromParts: { "year" : "$year", "month" : "$month", "day": "$day"} }""")),
      )),
      sort(orderBy(descending("date"))),
      project(fields(
          excludeId(),
          include("name"), 
          Document("formattedDate" -> Document("""{ $dateToString: { date: "$date", format: "%m/%d/%Y" } }"""))
      ))
    )      
  fromPartsCollection.aggregate(fromPartsPipeline).printResults()

  
  println("\nMake a collection with documents that define dates as strings in various formats...")
  val fromStringsDocuments = List(   
      Document("name" -> "Edward birth instant", "dateString" -> "2018-08-11T20:05:00.000"),
      Document("name" -> "Edward birth day only", "dateString" -> "2018-08-11"),
      Document("name" -> "Edward birth day in a different format", "dateString" -> "8/11/2018")
    )
  val fromStringsCollection: MongoCollection[Document] = database.getCollection("DatesFromStrings")
  fromStringsCollection.drop().results()
  fromStringsCollection.insertMany(fromStringsDocuments).results()
  fromStringsCollection.find().printResults()

  println("\nCompute dates from strings in different formats...")
  val fromStringsPipeline = Seq(
      project(fields(
          include("name", "dateString"), 
          Document("date" -> Document("""{ $dateFromString: { "dateString" : "$dateString" } }""")),
      )),
      project(fields(
          excludeId(),
          include("name", "dateString", "date"), 
          Document("formattedDate" -> Document("""{ $dateToString: { date: "$date", format: "%m/%d/%Y %H:%M" } }"""))
      ))
    )      
  fromStringsCollection.aggregate(fromStringsPipeline).printResults()
  

  println("\nMake a collection with documents that define dates as values in various formats...")
  val fromValuesDocuments = List(   
      Document("name" -> "An ISO date instance string", "dateValue" -> "2020-01-31T00:00:00Z"),
      Document("name" -> "A date string", "dateValue" -> "1/31/2020"),
      Document("name" -> "1000.1 ms after Unix Epoch", "dateValue" -> 1000.1),
      Document("""{ name: "1000.1 ms after Unix Epoch", dateValue: NumberDecimal("1000.1") }"""),
      Document("""{ name: "1000 ms after Unix Epoch", dateValue: NumberLong("1000") }"""),
      Document("""{ name: "An ObjectID made on 1/31/2020", dateValue: ObjectId("5e347b15cba19f5e654f7161") }""")
    )
  val fromValuesCollection: MongoCollection[Document] = database.getCollection("DatesFromValues")
  fromValuesCollection.drop().results()
  fromValuesCollection.insertMany(fromValuesDocuments).results()
  fromValuesCollection.find().printResults()

  println("\nCompute dates from values in different formats...")
  val fromValuesPipeline = Seq(
      project(fields(
          include("name", "dateValue"), 
          Document("date" -> Document("""{ $toDate: "$dateValue" }""")),
      )),
      project(fields(
          excludeId(),
          include("name", "dateValue", "date"), 
          Document("formattedDate" -> Document("""{ $dateToString: { date: "$date", format: "%m/%d/%Y %H:%M:%S.%L" } }"""))
      ))
    )      
  fromValuesCollection.aggregate(fromValuesPipeline).printResults()

  
  mongoClient.close()
}
