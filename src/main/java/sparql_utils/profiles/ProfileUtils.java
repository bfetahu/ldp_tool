/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sparql_utils.profiles;

import com.hp.hpl.jena.query.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;

/**
 *
 * @author besnik
 */
public class ProfileUtils {

	private final static Logger LOGGER = Logger.getLogger(ProfileUtils.class.getName());

    /**
     * Gets the resources from property vol:derivedFrom from the links in the
     * dataset profiles.
     *
     * @param uri
     * @param endpoint
     * @return
     */
    public static Set<String> getDerivedFromResources(String uri, String endpoint) {
        Set<String> derived_resources = new HashSet<String>();

        String querystr = "SELECT DISTINCT ?entity WHERE {GRAPH <http://data-observatory.org/lod-profiles/lod-cloud-profiles-full> {<" + uri + "> <http://purl.org/vol/ns/derivedFrom> ?entity}}";
        try {
            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(endpoint, query);
            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();
                derived_resources.add(qs.get("?entity").toString());
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return derived_resources;
    }

    /**
     * Get the dataset names from the profiles.
     *
     * @param profile_endpoint
     * @param graph_name
     * @return
     */
    public static Map<String, String> getDatasetProfileNames(String profile_endpoint, String graph_name) {
        Map<String, String> dataset_names = new TreeMap<String, String>();
        //load the all the dataset names from the linksets existing in the given profile
        String querystr = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dcterms:<http://purl.org/dc/terms/> PREFIX void:<http://rdfs.org/ns/void#> "
                + "SELECT ?linkset ?dataset_name WHERE {GRAPH <" + graph_name + "> {?dataset a void:Dataset. ?dataset dcterms:title ?dataset_name. ?linkset void:target ?dataset}}";

        try {
            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(profile_endpoint, query);
            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();
                dataset_names.put(qs.get("?linkset").toString(), qs.get("?dataset_name").toString());
            }
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
        return dataset_names;
    }

    /**
     * For a given linkset the method returns the top-k links under certain
     * filtering criteria that ensure that noise is filtered out.
     *
     * @param profile_endpoint
     * @param graph_name
     * @param linkset
     */
    public static Map<String, Entry<String, Double>> getTopKLinksFromDatasetProfile(String profile_endpoint, String graph_name, String linkset, int top_k, Set<String> categories_full) {
        Map<String, Entry<String, Double>> linkset_data = new TreeMap<String, Entry<String, Double>>();

        String querystr = "PREFIX vol:<http://purl.org/vol/ns/> "
                + "SELECT ?link ?category ?score WHERE { GRAPH <" + graph_name + "> {"
                + "<" + linkset + "> vol:hasLink ?link. "
                + "?link vol:linksResource ?category. "
                + "?link vol:hasScore ?score. "
                + "?link vol:derivedFrom ?entity. "
                + "FILTER (!regex(?category, \"[0-9](.*)\", \"i\") && !regex(?category, \"wikipedia\", \"i\") && !regex(?category, \"main_classification\", \"i\") && " 
                + "!regex(?category, \"wikipedia\", \"i\") && !regex(?category, \"people\", \"i\") && !regex(?category, \"universit\", \"i\")  && !regex(?category, \"christi\", \"i\") && " 
                + "!regex(?category, \"main_topic\", \"i\") && !regex(?category, \"categories\", \"i\") && !regex(?category, \"loanwords\", \"i\") && !regex(?category, \"nationality\", \"i\") && " 
                + "!regex(?category, \"countries\", \"i\") && !regex(?category, \"member\", \"i\"))}} "
                + "GROUP BY ?link ?category ?score "
                + "HAVING (COUNT(?entity) > 2) "
                + "ORDER BY DESC(?score) "
                + "LIMIT " + top_k;

        try {
            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(profile_endpoint, query);
            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();

                String link_uri = qs.get("?link").toString();
                String category_uri = qs.get("?category").toString();
                Double score = qs.get("?score").asLiteral().getDouble();

                //add the category to the global category list
                categories_full.add(category_uri);
                
                linkset_data.put(link_uri, new AbstractMap.SimpleEntry<String, Double>(category_uri, score));
            }
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }

        return linkset_data;
    }

    /**
     * Loads the set of entities that are associated with a category from a set of links.
     * @param profile_endpoint
     * @param graph_name
     * @param categories
     * @return 
     */
    public static Map<String, Set<String>> loadTopicEntities(String profile_endpoint, String graph_name, Set<String> categories) {
        Map<String, Set<String>> category_entities = new TreeMap<String, Set<String>>();

        for (String category : categories) {
            Set<String> sub_category_entities = category_entities.get(category);
            sub_category_entities = sub_category_entities == null ? new HashSet<String>() : sub_category_entities;
            category_entities.put(category, sub_category_entities);
            
            String querystr = "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX vol:<http://purl.org/vol/ns/> "
                    + "SELECT DISTINCT ?entity WHERE { GRAPH <" + graph_name + "> {"
                    + "?link a vol:Link. "
                    + "?link vol:linksResource <" + category + ">."
                    + "?link vol:derivedFrom ?entity}}";

            try {
                Query query = QueryFactory.create(querystr);
                QueryExecution qExe = QueryExecutionFactory.sparqlService(profile_endpoint, query);
                ResultSet results = qExe.execSelect();

                while (results.hasNext()) {
                    QuerySolution qs = results.next();
                    String entity_uri = qs.get("?entity").toString();
                    sub_category_entities.add(entity_uri);
                }
            } catch (Exception e) {
	            LOGGER.severe(e.getMessage());
            }
        }

        return category_entities;
    }
}
