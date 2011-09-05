package org.seacourt.htmlrender

import scala.math.{log, ceil}
import org.seacourt.disambiguator.{TopicElement}

object HTMLRender
{
    def wikiLink( topicName : String ) =
    {
        val wikiBase = "http://en.wikipedia.org/wiki/"
        wikiBase + (if (topicName.startsWith("Main:")) topicName.drop(5) else topicName)
    }
    
    def renderGroup( rootId : String, groupMembership : Seq[(Int, String, Boolean, Double)], klass : String ) =
    {
        <div style="padding:6px" class={klass} id={rootId}>
        {
            var colours = List( "#a0a040", "#40a0a0" )
            var greys = List( "#606060", "#a0a0a0" )
            for ( ((rank, name, primaryTopic, weight), i) <- groupMembership.sortWith( _._4 > _._4 ).zipWithIndex ) yield
            {
                //val fontSize = (17 + log(weight).toInt) min 10
                val reweight = (log(weight*10000.0) max 0.0)
                var styles = "text-decoration:none" :: "font-size: %.2f%%".format( 100.0 * (0.7 + reweight/15.0) ) :: Nil
                if ( primaryTopic )
                {
                    styles = "color: %s; font-weight: bold".format(colours.head) :: styles
                    colours = colours.tail ++ List(colours.head)
                }
                else
                {
                    styles = "color: %s".format(greys.head) :: styles
                    greys = greys.tail ++ List(greys.head)
                }
                
                <span class={klass} id={"%s_%d".format(rootId, i)}><a href={wikiLink(name)} style={styles.mkString(";")}>{ name.replace( "Main:", "" ) }</a></span><span> </span>
            }
        }
        </div>
    }
    
    def skillsTable( groupsByRank : Seq[Seq[(Int, TopicElement)]] ) =
    {
        <table>
        {
            val asArr = groupsByRank.toArray
            val numCells = asArr.size
            val numCols = 6
            val numRows = ceil(numCells.toDouble / numCols.toDouble).toInt
            var i = 0
            for ( r <- 0 until numRows ) yield
            {
                <tr>
                {
                    for ( c <- 0 until numCols ) yield
                    {
                        val cell = if ( i < asArr.size )
                        {
                            val tes = asArr(i)
                            
                            renderGroup( "g_%d".format(i), tes.zipWithIndex.map( x => {
                                val rank = x._2
                                val te = x._1._2
                                
                                (rank, te.name, te.primaryTopic, te.weight)
                            } ), "selectable" )
                        }
                        else <span/>
                        
                        i += 1
                        <td>{cell}</td>
                    }
                }
                </tr>
            }
        }
        </table>
    }
}
