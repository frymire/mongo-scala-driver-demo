package us.dac

import org.mongodb.scala.bson.BsonBinary

object BinaryHelpers {
  
  /** Returns a byte from a list of positions with bits to be set to 1. */
  def oneBits2Byte(ones: List[Int]) = if (ones.length == 0) { 0.toByte } else ones map { 1 << _ } reduce { _ | _ } toByte;

  /** Returns the byte in the specified position (MSB = 3, LSB = 0) of the provided Int. */
  def getByteFromInt(x: Int, byteIndex: Int): Byte = (x >> 8*byteIndex).toByte
  
  /** Returns a BsonBinary instance populated with a provided Byte. */
  def byte2Bson(b: Byte) = BsonBinary(Array(b))
  
  /** Returns an Int based on a binary string. */
  def binaryString2Int(str: String) = Integer.parseInt(str.replaceAll("\\s", ""), 2)

  /** Returns a string that results from prepending a provided string with 0s to reach a specified length. */
  def pad(str: String, nBits: Int) = {
    val unpaddedString = str.takeRight(nBits)
    ("0" * (nBits - unpaddedString.length)) + unpaddedString     
  }
  
  /** Returns a string representation of a byte, padded to eight characters. */
  def byte2String(b: Byte) = pad(b.toBinaryString, 8)
  
  /** Returns the number of bits in a byte that are set to 1. */
  def popcount(b: Byte) = pad(b.toBinaryString, 8) count { _ == '1' }
  
  /** Returns the number of bits in an Int that are set to 1. */
  def popcount2(i: Int) = pad(i.toBinaryString, 32) count { _ == '1' }

  /** Returns the number of flipped bits between two input bytes. */
  def hamming(b1: Byte, b2: Byte) = popcount((b1 ^ b2).toByte)
  
  /** Returns the number of flipped bits between two input Ints. */
  def hamming2(i1: Int, i2: Int) = popcount2(i1 ^ i2)  
}
