/**
 * Copyright 2014 Timothy Danford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package github.tdanford.ott

import java.io._
import scala.io._

object OTTTaxonomyLoader {

  val defaultHeaderFields : Array[String] =
    "uid\t|\tparent_uid\t|\tname\t|\trank\t|\tsourceinfo\t|\tuniqname\t|\tflags\t|\t".split("\t\\|\t")

  def parseLine( headerFields : Array[String], line : String ) : TaxonomyLine = {
    val map = headerFields.zip(line.split("\t\\|\t")).toMap
    TaxonomyLine(map("uid"), map("parent_uid"), map("name"),
      map("rank"), map("sourceinfo").split(","), map("uniqname"), map("flags").split(","))
  }

  def parseLine( line : String ) : TaxonomyLine = parseLine(defaultHeaderFields, line)
}

/**
 * Parses a taxonomy file, and turns it into a stream of TaxonomyLine objects.
 *
 * Will throw an exception if the local file parameter doesn't exist, is a directory, is
 * unreadable, or has no content.
 *
 * @param file A taxonomy file, conforming to the format described here:
 *             https://github.com/OpenTreeOfLife/reference-taxonomy/wiki/Interim-taxonomy-file-format
 */
class OTTTaxonomyLoader(file : File) extends Iterator[TaxonomyLine] {
  require(file.exists() && !file.isDirectory && file.canRead,
    "File %s is unreadable as a taxonomy file".format(file.getAbsolutePath))

  val src = Source.fromFile(file).getLines()
  assert(src.hasNext, "No lines in file %s".format(file.getAbsolutePath))

  private val headerLine = src.next()
  val headerFields : Array[String] = headerLine.split("\t\\|\t")
  assert(headerFields.length > 0, "Couldn't splite header line \"%s\"".format(headerLine))


  override def hasNext : Boolean = src.hasNext
  override def next() : TaxonomyLine = OTTTaxonomyLoader.parseLine(headerFields, src.next())
}

case class TaxonomyLine(uid : String,
                        parent_uid : String,
                        name : String,
                        rank : String,
                        sourceinfo : Array[String],
                        uniqname : String,
                        flags : Array[String]) {}
