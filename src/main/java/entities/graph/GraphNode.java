/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package entities.graph;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author besnik
 */
public class GraphNode implements Serializable{
    public String node_uri;
    public boolean isCategory;
    public int level;
    
    //store the different connections with other nodes
    public Set<Map.Entry<String, Double>> edge_weights;
    
    public GraphNode(){
        edge_weights = new HashSet<Map.Entry<String, Double>>();
    }
}