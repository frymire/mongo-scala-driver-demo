package us.dac.hamming

import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros.createCodecProviderIgnoreNone

import us.dac.BinaryHelpers._


/*
 * NOTE: The optimum number of subcodes is the code length divided by log-base-2(numDocuments).
 *  
 * -- For 16-bit codes over 2^8 = 256 records, use 16/8 = 2 subcodes of 8 bits
 * -- For 24-bit codes over 2^8 = 256 records, use 24/8 = 3 subcodes of 8 bits 
 * -- For 32-bit codes over 2^8 = 256 records, use 32/8 = 4 subcodes of 8 bits (too few records to be useful).
 * 
 * -- For 32-bit codes over 2^16 = 65,536 records, use 32/16 = 2 subcodes of 16 bits 
 * -- For 48-bit codes over 2^16 = 65,536 records, use 48/16 = 3 subcodes of 16 bits 
 * -- For 64-bit codes over 2^16 = 65,536 records, use 64/16 = 4 subcodes of 16 bits (maybe still too small to be useful).
 * 
 * -- For 48-bit codes over 2^24 = 16.78 million records, use 48/24 = 2 subcodes of 24 bits (2*6*16 = 192 MB without the index)
 * -- For 72-bit codes over 2^24 = 16.78 million records, use 72/24 = 3 subcodes of 24 bits (3*9*16 = 432 MB without the index)
 * -- For 96-bit codes over 2^24 = 16.78 million records, use 96/24 = 4 subcodes of 24 bits (4*12*16 = 768 MB without the index)
 * 
 * -- For 64-bit codes over 2^32 = 4.3 billion records, use 64/32 = 2 subcodes of 32 bits (2*8*4 = 64 GB without the index, too big)
 * -- For 96-bit codes over 2^32 = 4.3 billion records, use 96/32 = 3 subcodes of 32 bits (3*12*4 = 144 GB without the index, too big)
 */

trait Code {
  
  val code: Int // TODO: remove this from the high-level definition.
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


class SubcodeGenerator(val numBits: Int, val maxRadius: Int)  {
  
  val xBitBytes = (0 to maxRadius) map { n => (0 to numBits - 1).combinations(n).toList map { c => oneBits2Byte(c.toList) } } 

  def atDistance(distance: Int) = xBitBytes(distance)
  def withinRadius(radius: Int) = xBitBytes.slice(0, radius + 1).flatten  
    
  /** Returns a List of Bytes at a specified Hamming distance from the provided exemplar Byte. */
  def atDistanceXFrom(distance: Int, exemplar: Byte) = atDistance(distance) map { b => (b ^ exemplar).toByte }
  
  /** Returns a List of Bytes within a specified Hamming radius from the provided exemplar Byte. */
  def withinRadiusXFrom(radius: Int, exemplar: Byte) = withinRadius(radius) map { b => (b ^ exemplar).toByte }
  
  def printUpToRadius(radius: Int) = {
    
    (0 to radius) foreach { i =>
      println(s"\nList all bytes with $i bits set to 1...")
      xBitBytes(i) foreach { b => println(byte2String(b)) }    
    }

    println(s"\nList all bytes with $radius or fewer bits set to 1...")
    withinRadius(radius) foreach { b => println(byte2String(b)) }    
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
