package us.dac

import org.mongodb.scala._
import org.mongodb.scala.bson.annotations.BsonProperty
import org.mongodb.scala.bson.codecs.Macros.{createCodecProvider, createCodecProviderIgnoreNone}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._

import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

import us.dac.ObservableHelpers._


// Define a class to be used as a collection schema in the Mongo database.
// Just to be fancy, store the first name field in the database as "first" instead of the "firstName" variable name used by Scala.
case class Person(@BsonProperty("first") firstName: String, lastName: String, address: Option[String])

// To explicitly set Object IDs, do this.
//case class Person(_id: ObjectId, firstName: String, lastName: String, address: Option[String])
//object Person {
//  def apply(firstName: String, lastName: String, address: String): Person = Person(new ObjectId(), firstName, lastName, Some(address))
//  def apply(firstName: String, lastName: String): Person = Person(new ObjectId(), firstName, lastName, None)
//}

sealed class Tree
case class Branch(b1: Tree, b2: Tree, value: Int) extends Tree
case class Leaf(value: Int) extends Tree


object CaseClasses extends App {
  
  // Specify a codec registry to convert the Person and Tree types to and from BSON.
  val personCodecProvider = createCodecProviderIgnoreNone[Person]()
  val codecRegistry = fromRegistries(fromProviders(personCodecProvider, classOf[Tree]), DEFAULT_CODEC_REGISTRY)
  
  // Connect to the "MongoScalaDriverDemo" database while specifying the codec registry.
  // Note that if the database did not already exist, this call does not create it. It is only created when we write to it.
  val mongoClient = MongoClient()  
  val database = mongoClient.getDatabase("MongoScalaDriverDemo").withCodecRegistry(codecRegistry)
  
  // Make a new collection using the Person schema.
  val personCollection: MongoCollection[Person] = database.getCollection("People")
  personCollection.drop().results()
  
  println("\nAdd a single document using the person schema....")
  personCollection.insertOne(Person("Mark", "Frymire", Some("123 Main St."))).printResults()
  
  println("\nAdd several people....")
  val people = Seq(
        Person("Kurt", "Frymire", None),
        Person("Erick", "Frymire", None),
        Person("Edward", "Frymire", None)
      )      
  personCollection.insertMany(people).printResults()
  
  println("\nThe People collection after inserting one and then many Person documents...")
  personCollection.find().printResults()
  // Although Scala prints the Some and None values in the address field for all documents, that field is only present in the database
  // in documents for which it was explicitly provided. To see this, run "db.People.find()" from the Mongo shell.  
  
  println("\nUse a filter to query for a specific document...")
  personCollection.find(equal("firstName", "Erick")).printResults()
  
  println("\nQuery for a first names starting with 'E'...")
  personCollection.find(regex("firstName", "^E")).printResults()
  
  
  // Make a new collection using the hierarchical Tree schema.
  val treeCollection: MongoCollection[Tree] = database.getCollection("Tree")
  treeCollection.drop().results()
  
  println("\nAdd several trees....")
  val trees = Seq(
        Branch( Branch(Leaf(2), Leaf(1), 3), Branch(Leaf(7), Leaf(9), 16), 19),
        Leaf(5),
        Leaf(12)
      )      
  treeCollection.insertMany(trees).printResults()
  
  println("\nThe tree collection after inserting many Tree documents...")
  treeCollection.find().printResults()

  mongoClient.close()
}
