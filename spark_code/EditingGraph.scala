/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Displays graphs                                             //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import javax.swing.JFrame
import javax.swing.JMenu
import javax.swing.JRadioButton
import javax.swing.ButtonGroup
import javax.swing.JMenuBar
import javax.swing.JButton;
import javax.swing.JPanel;
import edu.uci.ics.jung.graph._
import scala.collection.JavaConversions._
import edu.uci.ics.jung.algorithms.layout._
import edu.uci.ics.jung.algorithms.layout.util._
import edu.uci.ics.jung.visualization.layout.LayoutTransition
import edu.uci.ics.jung.visualization._
import edu.uci.ics.jung.visualization.util._
import java.awt.BasicStroke
import java.awt.Color
import java.awt.Dimension
import java.awt.Paint
import java.awt.Stroke
import org.apache.commons.collections15._
import org.apache.commons.collections15.functors.ConstantTransformer
import edu.uci.ics.jung.visualization.decorators._
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel._
import edu.uci.ics.jung.visualization.control._
import org.apache.spark.graphx.Edge

object Network 
{        
    def networkCreation[ED](graph : Array[Edge[ED]]) 
    {	
		var g = new UndirectedSparseGraph[String, String]()
		graph.foreach
		{ 
			edge =>
			g.addVertex(edge.srcId.toString)
			g.addVertex(edge.dstId.toString)
			g.addEdge("From " + edge.srcId.toString + " to " + edge.dstId.toString, edge.srcId.toString, edge.dstId.toString)
		}
		
		var layout = new CircleLayout(g)

		layout.setSize(new Dimension(800,600));
		var vv = new VisualizationViewer[String,String](layout)
		
		vv.setPreferredSize(new Dimension(800,600))
		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller())
		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller())
		var gm = new DefaultModalGraphMouse()
		gm.setMode(ModalGraphMouse.Mode.TRANSFORMING)
		vv.setGraphMouse(gm)

		// Change the graph layouter
		def changeJUNGLayout(layout: AbstractLayout[String, String]): Unit = 
		{
	
			layout.setSize(vv.getSize())
	
			assert(vv.getModel().getRelaxer() eq null)
			if(layout.isInstanceOf[edu.uci.ics.jung.algorithms.util.IterativeContext]){
			  val relaxer: Relaxer
				= new VisRunner(
					layout.asInstanceOf[edu.uci.ics.jung.algorithms.util.IterativeContext])
			  relaxer.stop()
			  relaxer.prerelax()
			}
			
			{
			  val vs : Iterable[String] = layout.getGraph().getVertices()
			  for (v <- vs){
				  val minX = v.length()*5.0
				  val maxX = vv.getSize().getWidth() - minX
				  val X = layout.getX(v)
				  val newX = if (X < minX) minX else if (X > maxX) maxX else X
				  val minY = 12.5
				  val maxY = vv.getSize().getHeight() - minY
				  val Y = layout.getY(v)
				  val newY = if (Y < minY) minY else if (Y > maxY) maxY else Y
				  layout.setLocation(v, newX, newY)
			  }
			}
			
			val staticLayout: Layout[String, String] =
			new StaticLayout[String, String](g, layout)
			
			val lt: LayoutTransition[String,String] =
			new LayoutTransition[String,String](vv, vv.getGraphLayout(),
				staticLayout)
			val animator = new Animator(lt);
			animator.start();
			
			
			vv.repaint();
		}

		// Change the color of the vertex
		var vertexPaint = new Transformer[String,Paint]() 
		{
			def transform(i : String) : Paint = 
			{
				return Color.RED
			}
		}
		
		//~ // Set up a new stroke Transformer for the edges
		//~ var dash = Array(10.toFloat)
		//~ var edgeStroke = new BasicStroke(1.toFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.toFloat, line, 0.toFloat)
		//~ var edgeStrokeTransformer = new Transformer[String, Stroke]() 
		//~ {
			//~ def transform(s : String ): Stroke = 
			//~ {
				//~ return edgeStroke
			//~ }
		//~ }
		
		vv.getRenderContext().setVertexFillPaintTransformer(vertexPaint)
		//~ vv.getRenderContext().setEdgeStrokeTransformer(edgeStrokeTransformer)
		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller())
		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller())
		vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR)
	 
	 
        var frame = new JFrame("Editing and interactive Graph Viewer 1")
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
        frame.getContentPane().add(vv)
        
        // It adds a menu for changing mouse modes
        var menuBar = new JMenuBar()
        
        var modeMenu = gm.getModeMenu()
        modeMenu.setText("Mouse Mode")
        modeMenu.setIcon(null)
        modeMenu.setPreferredSize(new Dimension(150,30)) // Changes the size in order to make the text big enough
		menuBar.add(modeMenu)
        
        menuBar.add(modeMenu)
        frame.setJMenuBar(menuBar)
        gm.setMode(ModalGraphMouse.Mode.EDITING) // Start off in editing mode
        frame.pack()
        frame.setVisible(true); 
    }
    
    
    def treeCreation[ED](graph : Array[Edge[ED]], root : String) 
    {	
		var dg = new DirectedSparseGraph[String, String]()
		graph.foreach
		{ 
			edge =>
			dg.addVertex(edge.srcId.toString)
			dg.addVertex(edge.dstId.toString)
			dg.addEdge("From " + edge.srcId.toString + " to " + edge.dstId.toString, edge.srcId.toString, edge.dstId.toString)
		}
		
		var g = new DelegateTree(dg)
		g.setRoot(root)
		var layout = new TreeLayout(g)

		//~ layout.setSize(new Dimension(800,600));
		var vv = new VisualizationViewer[String,String](layout)
		
		//~ vv.setPreferredSize(new Dimension(800,600))
		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller())
		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller())
		var gm = new DefaultModalGraphMouse()
		gm.setMode(ModalGraphMouse.Mode.TRANSFORMING)
		vv.setGraphMouse(gm)

		// Change the graph layouter
		def changeJUNGLayout(layout: AbstractLayout[String, String]): Unit = 
		{
	
			layout.setSize(vv.getSize())
	
			assert(vv.getModel().getRelaxer() eq null)
			if(layout.isInstanceOf[edu.uci.ics.jung.algorithms.util.IterativeContext]){
			  val relaxer: Relaxer
				= new VisRunner(
					layout.asInstanceOf[edu.uci.ics.jung.algorithms.util.IterativeContext])
			  relaxer.stop()
			  relaxer.prerelax()
			}
			
			{
			  val vs : Iterable[String] = layout.getGraph().getVertices()
			  for (v <- vs){
				  val minX = v.length()*5.0
				  val maxX = vv.getSize().getWidth() - minX
				  val X = layout.getX(v)
				  val newX = if (X < minX) minX else if (X > maxX) maxX else X
				  val minY = 12.5
				  val maxY = vv.getSize().getHeight() - minY
				  val Y = layout.getY(v)
				  val newY = if (Y < minY) minY else if (Y > maxY) maxY else Y
				  layout.setLocation(v, newX, newY)
			  }
			}
			
			val staticLayout: Layout[String, String] =
			new StaticLayout[String, String](g, layout)
			
			val lt: LayoutTransition[String,String] =
			new LayoutTransition[String,String](vv, vv.getGraphLayout(),
				staticLayout)
			val animator = new Animator(lt);
			animator.start();
			
			
			vv.repaint();
		}

		// Change the color of the vertex
		var vertexPaint = new Transformer[String,Paint]() 
		{
			def transform(i : String) : Paint = 
			{
				return Color.RED
			}
		}
		
		//~ // Set up a new stroke Transformer for the edges
		//~ var dash = Array(10.toFloat)
		//~ var edgeStroke = new BasicStroke(1.toFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.toFloat, line, 0.toFloat)
		//~ var edgeStrokeTransformer = new Transformer[String, Stroke]() 
		//~ {
			//~ def transform(s : String ): Stroke = 
			//~ {
				//~ return edgeStroke
			//~ }
		//~ }
		
		vv.getRenderContext().setVertexFillPaintTransformer(vertexPaint)
		//~ vv.getRenderContext().setEdgeStrokeTransformer(edgeStrokeTransformer)
		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller())
		vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller())
		vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR)
		vv.getRenderContext().setEdgeShapeTransformer(new EdgeShape.Line())

		
        var frame = new JFrame("Editing and interactive Graph Viewer 1")
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
        frame.getContentPane().add(vv)
        
        // It adds a menu for changing mouse modes
        var menuBar = new JMenuBar()
        
        var modeMenu = gm.getModeMenu()
        modeMenu.setText("Mouse Mode")
        modeMenu.setIcon(null)
        modeMenu.setPreferredSize(new Dimension(150,30)) // Changes the size in order to make the text big enough
		menuBar.add(modeMenu)
        
        menuBar.add(modeMenu)
        frame.setJMenuBar(menuBar)
        gm.setMode(ModalGraphMouse.Mode.EDITING) // Start off in editing mode
        frame.pack()
        frame.setVisible(true); 
    }
}
