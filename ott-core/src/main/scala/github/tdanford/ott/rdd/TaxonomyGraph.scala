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
package github.tdanford.ott.rdd

import github.tdanford.ott.TaxonomyLine
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.math._

object TaxonomyGraph {

  def asGraph( taxons : RDD[TaxonomyLine] ) : Graph[TaxonomyLine,Int] = {
    val nodes : RDD[(VertexId,TaxonomyLine)] =
      taxons.map( taxLine => (taxLine.uid.toLong, taxLine) )
    val relationships : RDD[Edge[Int]] =
      taxons
        .filter(_.parent_uid.length > 0) // ignore the root of the tree
        .flatMap( taxLine => Seq(
        Edge(taxLine.uid.toLong, taxLine.parent_uid.toLong, -1),
        Edge(taxLine.parent_uid.toLong, taxLine.uid.toLong, 1) ))
    Graph(nodes, relationships)
  }

  case class Path(first : Long, rest : Option[Path] = None) {

    final def firstNode : Long = first

    @tailrec final def lastNode : Long = rest match {
      case Some(p) => p.lastNode
      case None => first
    }

    @tailrec private def innerLength(acc : Int) : Int = rest match {
      case Some(p) => p.innerLength(acc + 1)
      case None => acc
    }

    def length() : Int = innerLength(0)

    def nodes() : Seq[Long] = {
      rest match {
        case Some(p) => first +: p.nodes()
        case None => Seq(first)
      }
    }


  }

  def findPath( g : Graph[TaxonomyLine,Int], uid1 : String, uid2 : String ) : Path = {

    def minPath( p1 : Path, p2 : Path ) : Path = if(p1.length() <= p2.length()) p1 else p2
    def minOptionPath( p1 : Option[Path], p2 : Option[Path] ) : Option[Path] =
      (p1 ++ p2).reduceOption(minPath).headOption

    val (uid1Long, uid2Long) = (uid1.toLong, uid2.toLong)
    val initialGraph = g
      .mapVertices( ( id : Long, line : TaxonomyLine ) => if(uid1Long == id) Some(Path(id)) else None )
      .mapEdges(edge => 1)

    val sssp = initialGraph.pregel[Option[Path]](None)(
      (id, path : Option[Path], newPath : Option[Path]) => minOptionPath(path, newPath),
      triplet => {
        if (triplet.srcAttr.isDefined &&
          triplet.srcAttr.get.length() + 1 < triplet.dstAttr.map(_.length()).getOrElse(Int.MaxValue)) {
          Iterator((triplet.dstId, Some(Path(triplet.dstId, triplet.srcAttr))))
        } else {
          Iterator.empty
        }
      },
      (a,b) => minOptionPath(a,b)
    )

    val target = sssp.vertices.filter {
      case (id : graphx.VertexId, path : Option[Path]) => id == uid2Long
    }.collect()

    if(!target.head._2.isDefined) throw new IllegalStateException("No apparent path between %s and %s".format(uid1, uid2))
    target.head._2.get
  }

  def findPathToRoot( g : Graph[TaxonomyLine,Int], uid1 : String, uid2 : String ) : Path = {

    def minPath( p1 : Path, p2 : Path ) : Path = if(p1.length() <= p2.length()) p1 else p2
    def minOptionPath( p1 : Option[Path], p2 : Option[Path] ) : Option[Path] =
      (p1 ++ p2).reduceOption(minPath).headOption

    val (uid1Long, uid2Long) = (uid1.toLong, uid2.toLong)
    val initialGraph = g
      .mapVertices( ( id : Long, line : TaxonomyLine ) => if(uid1Long == id) Some(Path(id)) else None )
      .mapEdges(edge => 1)

    val sssp = initialGraph.pregel[Option[Path]](None, activeDirection=EdgeDirection.Out)(
      (id, path : Option[Path], newPath : Option[Path]) => minOptionPath(path, newPath),
      triplet => {
        if (triplet.srcAttr.isDefined &&
          triplet.srcAttr.get.length() + 1 < triplet.dstAttr.map(_.length()).getOrElse(Int.MaxValue)) {
          Iterator((triplet.dstId, Some(Path(triplet.dstId, triplet.srcAttr))))
        } else {
          Iterator.empty
        }
      },
      (a,b) => minOptionPath(a,b)
    )

    sssp.vertices.filter {
      case (id : graphx.VertexId, path : Option[Path]) => id == uid2Long
    }.collect().head._2.get
  }

  def findDistance( g : Graph[TaxonomyLine,Int], uid1 : String, uid2 : String ) : Int = {
    val (uid1Long, uid2Long) = (uid1.toLong, uid2.toLong)
    val initialGraph = g
      .mapVertices( (id : Long, line : TaxonomyLine) => if(uid1Long == id) 0 else Int.MaxValue )
      .mapEdges(edge => 1)

    val sssp = initialGraph.pregel(Int.MaxValue)(
      (id, dist : Int, newDist : Int) => min(dist, newDist),
      triplet => {
        // Had to modify the GraphX SSSP example wih the clause
        // ... triplet.srcAttr != Int.MaxValue ...
        // otherwise I was getting overflow answers when MaxValue + [some Number]
        // looped around and became negative :-(
        // Intuitively, if triplet.srcAttr == Int.MaxValue, then the src node is unreachable
        // and we shouldn't be sending any messages to the target node anyway.
        if (triplet.srcAttr != Int.MaxValue && triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b)
    )

    sssp.vertices.filter {
      case (id : graphx.VertexId, dist : Int) => id == uid2Long
    }.collect().head._2
  }
}
