/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.prgarnett.neo4jpanamacsvimport;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

/**
 *
 * @author philip
 */
public class LoadCSVs 
{
    private final GraphDatabaseService graphDB;
    private IndexDefinition indexDefinition;
    private File addresses;
    private File edges;
    private File entities;
    private File intermediaries;
    private File officers;
    private final String dirPath;
    
    public LoadCSVs(String dirPath)
    {
        if(!dirPath.endsWith("/"))
        {
            dirPath = dirPath.concat("/");
        }
        this.dirPath = dirPath;
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dirPath + "graphDB"));
        LoadCSVs.registerShutdownHook(graphDB);
    }
    
    public void loadTheCSVFiles()
    {
        this.addresses = new File(dirPath + "Addresses.csv");
        this.edges = new File(dirPath + "all_edges.csv");
        this.entities = new File(dirPath + "Entities.csv");
        this.intermediaries = new File(dirPath + "Intermediaries.csv");
        this.officers = new File(dirPath + "Officers.csv");
    }
    
    
    
    public void makeIndexs()
    {
        try ( Transaction tx = graphDB.beginTx() )
        {
            Schema schema = graphDB.schema();
            if(checkIndexes("node_id")==false)
            {
                indexDefinition = schema.indexFor( Label.label( "Panama" ) )
                        .on( "node_id" )
                        .create();
            }
            if(checkIndexes("countries")==false)
            {
                indexDefinition = schema.indexFor( Label.label( "Panama" ) )
                        .on( "countries" )
                        .create();
            }
            if(checkIndexes("country_codes")==false)
            {
                indexDefinition = schema.indexFor( Label.label( "Panama" ) )
                        .on("country_codes" )
                        .create();
            }
            if(checkIndexes("name")==false)
            {
                indexDefinition = schema.indexFor( Label.label( "Panama" ) )
                        .on("name" )
                        .create();
            }
            if(checkIndexes("address")==false)
            {
                indexDefinition = schema.indexFor( Label.label( "Panama" ) )
                        .on("address" )
                        .create();
            }
            tx.success();
        }
    }
    
    private boolean checkIndexes(String thisKey)
    {
        boolean found = false;
        Iterable<IndexDefinition> indexs;
        
        
        try (Transaction tx = graphDB.beginTx())
        {
            indexs = graphDB.schema().getIndexes(Label.label( "Panama" ));
            for(IndexDefinition indexD : indexs)
            {
                Iterable<String> theKeys = indexD.getPropertyKeys();
                for(String aKey : theKeys)
                {
                    if(aKey.equals(thisKey))
                    {
                        found = true;
                        break;
                    }
                }
                if(found)
                {
                    break;
                }
            }
            
            tx.success();
        }
        return found;
    }
    
    
    public void loadTheNodes()
    {
        System.out.println("Adding the Nodes and Edges");
        
        try ( Transaction tx = graphDB.beginTx() )
        {
            System.out.println("Read the Addresses");

            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader("address","icij_id", "valid_until", "country_codes", "countries", "node_id", "sourceID").parse(new FileReader(addresses));
            
            for(CSVRecord record : records)
            {
                if(!record.get("node_id").equals("node_id"))
                {
                    Node startNode = graphDB.createNode(Label.label( "Panama" ), Label.label( "Addresses" ));
                    startNode.setProperty("address", record.get("address"));
                    startNode.setProperty("icij_id", record.get("icij_id"));
                    startNode.setProperty("valid_until", record.get("valid_until"));
                    startNode.setProperty("country_codes", record.get("country_codes"));
                    startNode.setProperty("countries", record.get("countries"));
                    startNode.setProperty("node_id", Integer.parseInt(record.get("node_id")));
                    startNode.setProperty("sourceID", record.get("sourceID"));


                    System.out.println(startNode.getProperty("node_id").toString() + "\n\t" + startNode.getProperty("address").toString());
                }
            } //make the nodes

            tx.success();
        }
        catch (IOException | NumberFormatException e)
        {
            System.err.println("Error: "+ e.getMessage());
        }
        
        try ( Transaction tx = graphDB.beginTx() )
        {
            System.out.println("Read the Entities");

            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader("name","original_name","former_name","jurisdiction","jurisdiction_description","company_type","address","internal_id", 
                    "incorporation_date","inactivation_date","struck_off_date","dorm_date","status","service_provider","ibcRUC","country_codes","countries","note","valid_until","node_id","sourceID").parse(new FileReader(entities));
            
            for(CSVRecord record : records)
            {
                if(!record.get("node_id").equals("node_id"))
                {
                    Node startNode = graphDB.createNode(Label.label( "Panama" ), Label.label( "Entities" ));
                    startNode.setProperty("name", record.get("name"));
                    startNode.setProperty("original_name", record.get("original_name"));
                    startNode.setProperty("former_name", record.get("former_name"));
                    startNode.setProperty("jurisdiction", record.get("jurisdiction"));
                    startNode.setProperty("jurisdiction_description", record.get("jurisdiction_description"));
                    startNode.setProperty("company_type", record.get("company_type"));
                    startNode.setProperty("address", record.get("address"));
                    startNode.setProperty("internal_id", record.get("internal_id"));
                    startNode.setProperty("incorporation_date", record.get("incorporation_date"));
                    startNode.setProperty("inactivation_date", record.get("inactivation_date"));
                    startNode.setProperty("struck_off_date", record.get("struck_off_date"));
                    startNode.setProperty("dorm_date", record.get("dorm_date"));
                    startNode.setProperty("status", record.get("status"));
                    startNode.setProperty("service_provider", record.get("service_provider"));
                    startNode.setProperty("ibcRUC", record.get("ibcRUC"));
                    startNode.setProperty("country_codes", record.get("country_codes"));
                    startNode.setProperty("countries", record.get("countries"));
                    startNode.setProperty("note", record.get("note"));
                    startNode.setProperty("valid_until", record.get("valid_until"));
                    startNode.setProperty("node_id", Integer.parseInt(record.get("node_id")));
                    startNode.setProperty("sourceID", record.get("sourceID"));


                    System.out.println(startNode.getProperty("node_id").toString() + "\n\t" + startNode.getProperty("name").toString());
                }
            } //make the nodes

            tx.success();
        }
        catch (IOException | NumberFormatException e)
        {
            System.err.println("Error: "+ e.getMessage());
        }
        
        try ( Transaction tx = graphDB.beginTx() )
        {
            System.out.println("Read the Intermediaries");

            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader("name","internal_id", "address", "valid_until","country_codes","countries","status","node_id","sourceID").parse(new FileReader(intermediaries));
            
            for(CSVRecord record : records)
            {
                if(!record.get("node_id").equals("node_id"))
                {
                    Node startNode = graphDB.createNode(Label.label( "Panama" ), Label.label( "Intermediaries" ));
                    startNode.setProperty("name", record.get("name"));
                    startNode.setProperty("internal_id", record.get("internal_id"));
                    startNode.setProperty("address", record.get("address"));
                    startNode.setProperty("valid_until", record.get("valid_until"));
                    startNode.setProperty("country_codes", record.get("country_codes"));
                    startNode.setProperty("countries", record.get("countries"));
                    startNode.setProperty("status", record.get("status"));
                    startNode.setProperty("node_id", Integer.parseInt(record.get("node_id")));
                    startNode.setProperty("sourceID", record.get("sourceID"));                

                    System.err.println(startNode.getProperty("node_id").toString() + "\n\t" + startNode.getProperty("name").toString());
                }
            } //make the nodes

            tx.success();
        }
        catch (IOException | NumberFormatException e)
        {
            System.out.println("Error: "+ e.getMessage());
        }
        
        try ( Transaction tx = graphDB.beginTx() )
        {
            System.out.println("Read the Officers");

            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader("name","icij_id","valid_until","country_codes","countries","node_id","sourceID").parse(new FileReader(officers));
            
            for(CSVRecord record : records)
            {
                if(!record.get("node_id").equals("node_id"))
                {
                    Node startNode = graphDB.createNode(Label.label( "Panama" ), Label.label( "Officers" ));
                    startNode.setProperty("name", record.get("name"));
                    startNode.setProperty("icij_id", record.get("icij_id"));
                    startNode.setProperty("valid_until", record.get("valid_until"));
                    startNode.setProperty("country_codes", record.get("country_codes"));
                    startNode.setProperty("countries", record.get("countries"));
                    startNode.setProperty("node_id", Integer.parseInt(record.get("node_id")));
                    startNode.setProperty("sourceID", record.get("sourceID"));                

                    System.out.println(startNode.getProperty("node_id").toString() + "\n\t" + startNode.getProperty("name").toString());
                }
            } //make the nodes

            tx.success();
        }
        catch (IOException | NumberFormatException e)
        {
            System.err.println("Error: "+ e.getMessage());
        }
    }
    
    public void makeTheEdges()
    {
        try ( Transaction tx = graphDB.beginTx() )
        {
            System.out.println("Read the Edges (Relationships)");

            Iterable<CSVRecord> records = CSVFormat.RFC4180.withHeader("node_1","rel_type","node_2").parse(new FileReader(edges));
            
            for(CSVRecord record : records)
            {
                Node sourceNode = this.getNode(Integer.parseInt(record.get("node_1")));
                Node desNode = this.getNode(Integer.parseInt(record.get("node_2")));

                Relationship rel = sourceNode.createRelationshipTo(desNode, RelationshipType.withName(record.get("rel_type")));
                System.out.println(rel.getStartNode().getId() + "<->" + rel.getEndNode().getId());
            }
            
            tx.success();
        }
        catch (IOException | NumberFormatException e)
        {
            System.out.println(e.getMessage());
        }
    }
    
    
    private Node getNode(Integer nodeID)
    {
        Label label = Label.label( "Panama" );
        return graphDB.findNode(label, "node_id", nodeID);
    }
    
    
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
