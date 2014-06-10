/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sparql_utils.dbpedia.category_entity_utils;

import com.hp.hpl.jena.query.*;

import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 *
 * @author besnik
 */
public class DBpediaCatEnt {

	private final static Logger LOGGER = Logger.getLogger(DBpediaCatEnt.class.getName());

    /**
     * Loads analytics about the a specific category, in this case it loads the
     * number of occurrences for each resource type, exploiting the information
     * from rdf:type from an entity.
     *
     * @param category_uri
     * @param dbpedia_url
     * @return
     */
    public static Map<String, Integer> loadEntityTypeCountForCategory(String category_uri, String dbpedia_url) {
        Map<String, Integer> type_count = new TreeMap<String, Integer>();

        String query_str = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dcterms: <http://purl.org/dc/terms/>"
                + "SELECT ?type (COUNT(?type) as ?count_type) WHERE {?entity dcterms:subject <" + category_uri + ">. ?entity a ?type} ORDER BY DESC(?count_type) LIMIT 1000";
        try {
            Query query = QueryFactory.create(query_str);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);
            ResultSet results = qExe.execSelect();
            while (results.hasNext()) {
                QuerySolution qs = results.next();
                type_count.put(qs.get("?type").toString(), qs.get("?count_type").asLiteral().getInt());
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return type_count;
    }

    
}
