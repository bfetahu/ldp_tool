package sparql_utils.dbpedia;

import com.hp.hpl.jena.query.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

public class DBPedia {

	private final static Logger LOGGER = Logger.getLogger(DBPedia.class.getName());

    /**
     * Extract the categories of an entity.
     *
     * @param entity
     * @param dbpedia_url
     * @return
     */
    public static Set<String> getEntityCategories(String entity, String dbpedia_url) {
        Set<String> categories = new HashSet<String>();
        String querystr = "SELECT DISTINCT ?category WHERE {GRAPH <http://dbpedia.org/articles> {<" + entity + "> <http://purl.org/dc/terms/subject> ?category}}";
        try {
            Query query = QueryFactory.create(querystr); //s2 = the query above
            QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);
            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();
                categories.add(qs.get("?category").toString());
            }
        } catch (Exception e) {
	        LOGGER.severe(entity + "\t"+ e.getMessage());
        }

        return categories;
    }

    /**
     * Loads the set of entities that have assigned a certain category.
     * Furthermore, we extract all the other categories assigned to those
     * matching entities.
     *
     * This is used later on to compute the conditional probabilities of the
     * categories.
     *
     * @param category
     * @param dbpedia_url
     * @return
     */
    public static List<Entry<String, String>> getCategoryEntities(String category, String dbpedia_url) {
        List<Entry<String, String>> category_entities = new ArrayList<Entry<String, String>>();

        String querystr = "SELECT ?entity ?category WHERE {GRAPH <http://dbpedia.org/articles> {?entity <http://purl.org/dc/terms/subject> <" + category + ">. ?entity <http://purl.org/dc/terms/subject> ?category}}";
        try {
            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);
            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();
                category_entities.add(new SimpleEntry<String, String>(qs.get("?entity").toString(), qs.get("?category").toString()));
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return category_entities;
    }
}
