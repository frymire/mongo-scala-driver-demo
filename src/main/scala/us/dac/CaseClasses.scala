package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Filters._
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

import us.dac.Helpers._


// Define a class to be used as a collection schema in the Mongo database.
object Person {
  def apply(firstName: String, lastName: String): Person = Person(new ObjectId(), firstName, lastName)
}
case class Person(_id: ObjectId, firstName: String, lastName: String)


object CaseClasses extends App {
  
  // Specify a codec registry to convert the Person type to and from BSON.
  val codecRegistry = fromRegistries(fromProviders(classOf[Person]), DEFAULT_CODEC_REGISTRY)
  
  // Connect to the "MongoScalaDriverDemo" database while specifying the codec registry.
  // Note that if the database did not already exist, this call does not create it. It is only created when we write to it.
  val mongoClient = MongoClient()  
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(codecRegistry)
  
  // Make a new collection using the Person schema.
  val collection: MongoCollection[Person] = database.getCollection("People")
  collection.drop().results()
  
  println("\nAdd a single document using the person schema....")
  collection.insertOne(Person("Mark", "Frymire")).printResults()
  
  println("\nAdd several people....")
  val people = Seq(
        Person("Kurt", "Frymire"),
        Person("Erick", "Frymire"),
        Person("Edward", "Frymire")
      )      
  collection.insertMany(people).printResults()
  
  println("\nThe collection after inserting one and then many Person documents...")
  collection.find().printResults()
  
  println("\nUse a filter to query for a specific document...")
  collection.find(equal("firstName", "Erick")).printResults()
  
  println("\nQuery for a first names starting with 'E'...")
  collection.find(regex("firstName", "^E")).printResults()
  
  mongoClient.close()
}