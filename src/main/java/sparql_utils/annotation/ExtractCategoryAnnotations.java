package sparql_utils.annotation;

import com.hp.hpl.jena.query.*;
import entities.metadata.CategoryAnnotation;

import java.util.logging.Logger;


public class ExtractCategoryAnnotations {

    private final static Logger LOGGER = Logger.getLogger(ExtractCategoryAnnotations.class.getName());

    /**
     * Extract the categories for a given entity resource.
     *
     * @param entityuri
     * @param timeout
     * @param dbpedia_url
     * @return
     */
    public CategoryAnnotation getCategories(String entityuri, long timeout, String dbpedia_url) {
        try {
            String querystr =
                    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> PREFIX dcterms: <http://purl.org/dc/terms/> "
                            + " SELECT DISTINCT ?cat ?cat1 ?cat2 WHERE {"
                            + " GRAPH <http://dbpedia.org/articles> {<" + entityuri.trim() + "> dcterms:subject ?cat}. "
                            + " GRAPH <http://dbpedia.org> {?cat skos:broader ?cat1}}";
            //update the entity information.
            CategoryAnnotation catant = new CategoryAnnotation();
            catant.categoryname = "Main_Classification";
            catant.level = 0;

            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);

            ResultSet results = qExe.execSelect();

            while (results.hasNext()) {
                QuerySolution qs = results.next();
                String cat0 = "", cat1 = "";
                CategoryAnnotation catant0 = null, catant1 = null;

                if (qs.contains("?cat")) {
                    cat0 = qs.get("?cat").toString();
                    catant0 = hasCategory(cat0, 1, catant);
                }
                if (qs.contains("?cat1")) {
                    cat1 = qs.get("?cat1").toString();
                    catant1 = hasCategory(cat1, 2, catant0);
                }
            }
            qExe.close();
            return catant;
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
        return null;
    }

    /**
     * @param category
     * @param level
     * @param cat
     * @return
     */
    private CategoryAnnotation hasCategory(String category, int level, CategoryAnnotation cat) {
        CategoryAnnotation catchild = null;
        if (cat.containsChild(category)) {
            for (CategoryAnnotation cattmp : cat.children) {
                if (cattmp.categoryname.equals(category)) {
                    catchild = cattmp;
                    break;
                }
            }
        } else {
            catchild = new CategoryAnnotation();
            catchild.categoryname = category;
            catchild.level = level;
            cat.children.add(catchild);
        }

        return catchild;
    }
}
