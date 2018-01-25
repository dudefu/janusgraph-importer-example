package net.mpolonioli.janusgraph.importer.csv.example;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

import net.mpolonioli.janusgraph.importer.csv.JanusGraphCsvImporter;

public class App 
{
    @SuppressWarnings("rawtypes")
	public static void main( String[] args ) throws IOException, ParseException, InterruptedException, ExecutionException {
    	String janusConf = args[0];
    	File vertexDir = new File(args[1]);
    	File edgeDir = new File(args[2]);
    	
    	/*
    	 * put in a List<File> all the files contained in the input directories
    	 */
    	List<File> vertexFile = listAllFiles(vertexDir);
    	List<File> edgeFile = listAllFiles(edgeDir);
    	
    	/*
    	 * create the HashMap<String, Class> declaring the corresponding type for each property label
    	 * supported types by now: Long.class, String.class, Integer.class, Boolean.class
    	 */
    	HashMap<String, Class> propertyHasType = new HashMap<>();
    	propertyHasType.put("id", Long.class);
    	propertyHasType.put("name", String.class);
    	propertyHasType.put("surname", String.class);
    	propertyHasType.put("email", String.class);
    	propertyHasType.put("date", String.class);
    	
    	/*
    	 * create the HashMap<String, Cardinality> declaring the corresponding cardinality for each property label
    	 * the Cardinality class must come from org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
    	 * supported cardinalities by now: Cardinality.single, Cardinality.list
    	 */
    	HashMap<String, Cardinality> propertyHasCardinality = new HashMap<>();
    	propertyHasCardinality.put("id", Cardinality.single);
    	propertyHasCardinality.put("name", Cardinality.single);
    	propertyHasCardinality.put("surname", Cardinality.single);
    	propertyHasCardinality.put("email", Cardinality.list);
    	propertyHasCardinality.put("date", Cardinality.single);
    	
    	/*
    	 * create the List<String> vertexLabels with all the vertex labels
    	 */
    	List<String> vertexLabels = new ArrayList<>();
    	vertexLabels.add("Person");
    	
    	/*
    	 * create the List<String> edgeLabels with all the edge labels
    	 */
    	List<String> edgeLabels = new ArrayList<>();
    	edgeLabels.add("knows");
    	
    	/*
    	 * create the List<String> singleCardPropKeys with all the property labels of properties with Cardinality.single
    	 */
    	List<String> singleCardPropKeys = new ArrayList<>();
    	singleCardPropKeys.add("id");
    	singleCardPropKeys.add("name");
    	singleCardPropKeys.add("surname");
    	singleCardPropKeys.add("date");
    	
    	/*
    	 * create the List<String> listCardPropKeys with all the property labels of properties with Cardinality.list
    	 */
    	List<String> listCardPropKeys = new ArrayList<>();
    	listCardPropKeys.add("email");
    	
    	/*
    	 * define the schema
    	 */
    	defineSchema(janusConf, vertexLabels, edgeLabels, singleCardPropKeys, listCardPropKeys, propertyHasType);
    	
    	/*
    	 * instance the JanusGraphCsvImporter passing the path to a janusgraph configuration file
    	 */
    	JanusGraphCsvImporter janusImporter = new JanusGraphCsvImporter(janusConf);
    	
    	/*
    	 * load all the vertices contained in each file of the list vertexFile
    	 */
    	for( File file : vertexFile )
    	{
        	janusImporter.loadVertices(file, true, 20000, 10, 1, propertyHasType, propertyHasCardinality);
    	}
    	    	
    	/*
    	 * load all the edges contained in each file of the list edgeFile
    	 */
    	for( File file : edgeFile )
    	{
    		janusImporter.loadEdges(file, new HashMap<>(), true, true, 20000, 10, 1, propertyHasType);
    	}
    	
    	janusImporter.closeConnection();
    }
    
	/*
	 * put in a list all files in the given directory at any level
	 */
	private static List<File> listAllFiles(File dir)
	{
		List<File> result = new ArrayList<>();
	    for (File entry : dir.listFiles()) {
	      if (entry.isDirectory())
	      {
	    	  result.addAll(listAllFiles(entry));
	      } else
	      {
	    	  result.add(entry);
	      }
	    }
	    return result;
	}
	
	/*
	 * clear the existing graph and define the data schema of the example
	 */
	private static void defineSchema(
			String janusConf,
			List<String> vertexLabels,
			List<String> edgeLabels,
			List<String> singleCardPropKeys,
			List<String> listCardPropKeys,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType
			)
	{
		JanusGraph graph = JanusGraphFactory.open(janusConf);
		
		graph.close();
		org.janusgraph.core.util.JanusGraphCleanup.clear(graph);
		graph = JanusGraphFactory.open(janusConf);
		JanusGraphManagement mgmt;
		
		// Declare all vertex labels
		System.out.println("Declaring all vertex labels");
		for( String vLabel : vertexLabels ) {
			System.out.print(vLabel + " ");
			mgmt = graph.openManagement();
			mgmt.makeVertexLabel(vLabel).make();
			mgmt.commit();
		}
		
		// Declare all edge labels
		System.out.println("\nDeclaring all edge labels");
		for( String eLabel : edgeLabels ) {
			System.out.print(eLabel + " ");
			mgmt = graph.openManagement();
			mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
			mgmt.commit();
		}
		
		// Declare all properties with Cardinality.SINGLE and an index for property "id"
		System.out.println("\nDeclaring all properties with Cardinality.SINGLE");
		for ( String propKey : singleCardPropKeys ) {
			System.out.print(propKey + " ");
			mgmt = graph.openManagement();
			PropertyKey property = mgmt.makePropertyKey(propKey).dataType(propertyHasType.get(propKey))
					.cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
			if(propKey.equals("id"))
			{
				String indexLabel = "byIdComposite";
				System.out.print(indexLabel + " ");
				mgmt.buildIndex(indexLabel, Vertex.class).addKey(property).buildCompositeIndex();
			}
			mgmt.commit();
		}
		
		// Declare all properties with Cardinality.LIST
		System.out.println("\nDeclaring all properties with Cardinality.LIST");
		for ( String propKey : listCardPropKeys ) {
			System.out.print(propKey + " ");
			mgmt = graph.openManagement();
			mgmt.makePropertyKey(propKey).dataType(propertyHasType.get(propKey))
			.cardinality(org.janusgraph.core.Cardinality.LIST).make();     
			mgmt.commit();
		}
		graph.tx().commit();
		System.out.println();
		graph.close();
	}
}
