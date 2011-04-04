import java.awt.Font
import java.awt.Graphics
import java.awt.Graphics2D
import java.awt.Insets
import java.awt.RenderingHints
import java.awt.font.FontRenderContext
import java.awt.font.LineBreakMeasurer
import java.awt.font.TextAttribute
import java.awt.font.TextLayout
import java.text.AttributedCharacterIterator
import java.text.AttributedString
import java.awt.Color._

import javax.swing.JFrame
import javax.swing.JPanel

class ParagraphLayout extends JPanel
{
    override def paint( g : Graphics )
    {
        g match
        {
            case g2 : Graphics2D =>
            {
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

                val s = "Java components and products Directory for Java components " +
                    "and applications.Hundreds of Java components and applications " +
                    "are organized by topic. You can find what you need easily. " +
                    "You may also compare your product with others. If your component " +
                    "is not listed, just send your url to java2s@java2s.com. " +
                    "http://www.java2s.com"
                    
                val font = new Font("Serif", Font.PLAIN, 14)
                val as = new AttributedString(s)
                as.addAttribute(TextAttribute.FONT, font)
                val aci = as.getIterator()

                val frc = g2.getFontRenderContext()
                val lbm = new LineBreakMeasurer(aci, frc)
                val insets = getInsets()
                val wrappingWidth = getSize().width - insets.left - insets.right
                var x : Float = insets.left
                var y : Float = insets.top


                
                while (lbm.getPosition() < aci.getEndIndex())
                {
                    val textLayout = lbm.nextLayout(wrappingWidth)
                    
                    // Allows us to keep track of which characters are where
                    val numChars = textLayout.getCharacterCount()
                    y += textLayout.getAscent()
                    
                    g2.setColor( black )
                    textLayout.draw(g2, x, y)
                    
                    // Can have start and end character positions   
                    val layoutShape = textLayout.getLogicalHighlightShape(0, 10)
                    val r = layoutShape.getBounds()
                    val lineY = y.toInt + r.y + r.height
                    
                    g2.setColor( red )
                    g2.drawLine( r.x, lineY, r.x + r.width, lineY )
                    
                    y += 2*(textLayout.getDescent() + textLayout.getLeading())
                    x = insets.left
                }
            }
            
            case _ => throw new ClassCastException
        }
    }
}

object ParagraphLayout
{
    def main( args : Array[String] )
    {
        val f = new JFrame()
        f.getContentPane().add( new ParagraphLayout() )
        f.setSize(350, 250)
        f.show()
    }
}

