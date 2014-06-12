package sparql_utils.annotation;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.sparql.lib.org.json.JSONArray;
import com.hp.hpl.jena.sparql.lib.org.json.JSONException;
import com.hp.hpl.jena.sparql.lib.org.json.JSONObject;
import entities.metadata.CategoryAnnotation;
import entities.metadata.DBPediaAnnotation;
import org.omg.PortableInterceptor.LOCATION_FORWARD;
import utils_lod.WebUtils;

import java.util.*;
import java.util.logging.Logger;


public class ExtractCategoryAnnotations {

	private final static Logger LOGGER = Logger.getLogger(ExtractCategoryAnnotations.class.getName());

    public void appendCategories(Map<String, DBPediaAnnotation> dbpconcepts, String categoryannotationurl, long timeout) {
        for (String dbpconcepturi : dbpconcepts.keySet()) {
            DBPediaAnnotation dbpa = dbpconcepts.get(dbpconcepturi);
	        LOGGER.info("Assigning categories for entity... " + dbpa.getAnnotationURI());

            //check if it is annotated previously
            if (dbpa.category.categoryname != null) {
                continue;
            }

            CategoryAnnotation cat = getCategories(dbpa.uri, timeout, categoryannotationurl);
            if (cat == null) {
                continue;
            }

            dbpa.category = cat;
        }
    }

    public CategoryAnnotation getCategoryAnnotations(DBPediaAnnotation dbpa, String categoryannotationurl) {
        CategoryAnnotation cat = new CategoryAnnotation();

        try {
            //read the returned content from the REST request.
            String response = WebUtils.request(categoryannotationurl + "?uri=" + dbpa.getAnnotationURI());
            JSONObject jsobj = new JSONObject(response);
            //parse the json text into a category hierarchy
            int level = 0;
            generateCategoryTree(cat, jsobj, level);
            return cat;
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return null;
    }

    /**
     * Construct recursively the annotation tree of categories for each entity.
     *
     * @param cat
     * @param jsobj
     * @param level
     */
    public void generateCategoryTree(CategoryAnnotation cat, JSONObject jsobj, int level) {
        try {
            cat.categoryname = jsobj.get("name").toString();
            cat.level = level;

            //check if it has children, otherwise break

            if (jsobj.getJSONArray("children") == null) {
                return;
            }


            JSONArray jarr = jsobj.getJSONArray("children");
            for (int i = 0; i < jarr.length(); i++) {
                JSONObject jsobjchild = jarr.getJSONObject(i);

                CategoryAnnotation catchild = new CategoryAnnotation();
                cat.children.add(catchild);
                generateCategoryTree(catchild, jsobjchild, ++level);
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Extract the categories for a given entity resource.
     *
     * @param categoryuri
     */
    public CategoryAnnotation getBroaderCategories(String categoryuri, long timeout, String dbpedia_url) {
        String querystr =
                "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + " SELECT DISTINCT ?cat ?cat1 ?cat2 ?cat3 "
                + " WHERE { GRAPH <http://dbpedia.org> {<" + categoryuri + ">  skos:broader ?cat. OPTIONAL {?cat skos:broader ?cat1}}} ";

        Query query = QueryFactory.create(querystr);
        QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);
        
        ResultSet results = qExe.execSelect();
        //update the entity information.
        CategoryAnnotation catant = new CategoryAnnotation();
        catant.categoryname = categoryuri;
        catant.level = 0;

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

        return catant;
    }
 /**
     * Extract the set of narrower categories for a given category. This is to
     * find the best matching DBpedia category subgraph for a particular
     * information need, like the resource type.
     *
     * @param entityuri
     */
    public Map<String, CategoryAnnotation> getTwoHopsNarrowCategories(CategoryAnnotation catant, long timeout, String dbpedia_url) {
        String querystr =
                "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + " SELECT DISTINCT ?parent_category ?child_category "
                + " WHERE { GRAPH <http://dbpedia.org> {?parent_category skos:broader <" + catant.categoryname + "> . ?child_category skos:broader ?parent_category}}";

        Query query = QueryFactory.create(querystr);
        QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);
        
        ResultSet results = qExe.execSelect();
       
        //store the index of already added categories.
        Map<String, CategoryAnnotation> categories_map = new HashMap<String, CategoryAnnotation>();
        categories_map.put(catant.categoryname, catant);

        while (results.hasNext()) {
            QuerySolution qs = results.next();

            if (qs.contains("?parent_category")) {
                String first_level_cat = qs.get("?parent_category").toString();
                CategoryAnnotation first_level_category = categories_map.get(first_level_cat);
                if (first_level_category == null) {
                    updateCategoryLevels(0, catant);
                    
                    first_level_category = new CategoryAnnotation();
                    first_level_category.categoryname = first_level_cat;
                    first_level_category.level = 0;
                    first_level_category.children.add(catant);
                    categories_map.put(first_level_cat, first_level_category);
                }
            }
            if (qs.contains("?child_category")) {
                String second_level_cat = qs.get("?child_category").toString();
                CategoryAnnotation second_level_category = categories_map.get(second_level_cat);
                if (second_level_category == null) {
                    CategoryAnnotation first_level_category = categories_map.get(qs.get("?parent_category").toString());
                    updateCategoryLevels(0, first_level_category);
                    
                    second_level_category = new CategoryAnnotation();
                    second_level_category.categoryname = second_level_cat;
                    second_level_category.level = 0;
                    second_level_category.children.add(first_level_category);
                    categories_map.put(second_level_cat, second_level_category);
                }
            }
        }
        
        //remove all non-leaf categories.
        Iterator<CategoryAnnotation> category_iterator = categories_map.values().iterator();
        while(category_iterator.hasNext()){
            CategoryAnnotation category = category_iterator.next();
            if(category.level != 0){
                category_iterator.remove();
            }
        }

        return categories_map;
    }
    /**
     * Extract the set of narrower categories for a given category. This is to
     * find the best matching DBpedia category subgraph for a particular
     * information need, like the resource type.
     *
     * @param entityuri
     */
    public Map<String, CategoryAnnotation> getOneHopNarrowCategories(CategoryAnnotation catant, long timeout, String dbpedia_url) {
        String querystr =
                "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + " SELECT DISTINCT ?parent_category  "
                + " WHERE { GRAPH <http://dbpedia.org> {?parent_category skos:broader <" + catant.categoryname + "> . ?child_category skos:broader ?parent_category}}";

        Query query = QueryFactory.create(querystr);
        QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);

        ResultSet results = qExe.execSelect();
       
        //store the index of already added categories.
        Map<String, CategoryAnnotation> categories_map = new HashMap<String, CategoryAnnotation>();
        categories_map.put(catant.categoryname, catant);

        while (results.hasNext()) {
            QuerySolution qs = results.next();

            if (qs.contains("?parent_category")) {
                String first_level_cat = qs.get("?parent_category").toString();
                CategoryAnnotation first_level_category = categories_map.get(first_level_cat);
                if (first_level_category == null) {
                    updateCategoryLevels(0, catant);
                    
                    first_level_category = new CategoryAnnotation();
                    first_level_category.categoryname = first_level_cat;
                    first_level_category.level = 0;
                    first_level_category.children.add(catant);
                    categories_map.put(first_level_cat, first_level_category);
                }
            }
        }

        return categories_map;
    }

    /**
     * Extract the set of broader categories for a given category. This is to
     * find the best matching DBpedia category subgraph for a particular
     * information need, like the resource type.
     *
     * @param categoryuri
     */
    public CategoryAnnotation getTwoHopsBroaderCategories(String categoryuri, long timeout, String dbpedia_url) {
        String querystr =
                "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + " SELECT DISTINCT ?parent_category ?child_category  "
                + " WHERE { GRAPH <http://dbpedia.org> {<" + categoryuri + "> skos:broader ?parent_category. ?child_category skos:broader ?parent_category}}";

        Query query = QueryFactory.create(querystr);
        QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);

        ResultSet results = qExe.execSelect();
        //update the entity information.
        CategoryAnnotation catant = new CategoryAnnotation();
        catant.categoryname = categoryuri;
        catant.level = 0;

        while (results.hasNext()) {
            QuerySolution qs = results.next();
            String parent_cat = "", child_cat = "";
            CategoryAnnotation parent_category = null;

            if (qs.contains("?parent_category")) {
                parent_cat = qs.get("?parent_category").toString();
                parent_category = hasCategory(parent_cat, 1, catant);
            }
            if (qs.contains("?child_category")) {
                child_cat = qs.get("?child_category").toString();
                hasCategory(child_cat, 2, parent_category);
            }
        }

        return catant;
    }
    
      /**
     * Extract the set of broader categories for a given category. This is to
     * find the best matching DBpedia category subgraph for a particular
     * information need, like the resource type.
     *
     * @param entityuri
     */
    public CategoryAnnotation getOneHopBroaderCategories(String categoryuri, long timeout, String dbpedia_url) {
        String querystr =
                "PREFIX dcterms: <http://purl.org/dc/terms/> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                + " SELECT DISTINCT ?parent_category  "
                + " WHERE { GRAPH <http://dbpedia.org> {<" + categoryuri + "> skos:broader ?parent_category. ?child_category skos:broader ?parent_category}}";

        Query query = QueryFactory.create(querystr);
        QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);

        ResultSet results = qExe.execSelect();
        //update the entity information.
        CategoryAnnotation catant = new CategoryAnnotation();
        catant.categoryname = categoryuri;
        catant.level = 0;

        while (results.hasNext()) {
            QuerySolution qs = results.next();
            String parent_cat = "", child_cat = "";
            CategoryAnnotation parent_category = null;

            if (qs.contains("?parent_category")) {
                parent_cat = qs.get("?parent_category").toString();
                parent_category = hasCategory(parent_cat, 1, catant);
            }
        }

        return catant;
    }

    /**
     * Extract the categories for a given entity resource.
     *
     * @param entityuri
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
     * Loads the directly related categories for an entity.
     *
     * @param entityuri
     * @param timeout
     * @param dbpedia_url
     * @return
     */
    public List<CategoryAnnotation> getFirstLevelCategories(String entityuri, long timeout, String dbpedia_url) {
        try {
            List<CategoryAnnotation> lst_first_level_categories = new ArrayList<CategoryAnnotation>();
            String querystr =
                    "PREFIX dcterms: <http://purl.org/dc/terms/> "
                    + " SELECT DISTINCT ?cat WHERE {GRAPH <http://dbpedia.org/articles> {<" + entityuri.trim() + "> dcterms:subject ?cat}}";

            Query query = QueryFactory.create(querystr);
            QueryExecution qExe = QueryExecutionFactory.sparqlService(dbpedia_url, query);

            ResultSet results = qExe.execSelect();

            //update the entity information.
            while (results.hasNext()) {
                QuerySolution qs = results.next();
                CategoryAnnotation category = new CategoryAnnotation();
                category.level = 1;
                category.categoryname = qs.get("?cat").toString();
                lst_first_level_categories.add(category);
            }

            return lst_first_level_categories;
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return null;
    }

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

    /**
     * Updates the level of categories.
     * @param base_level
     * @param category 
     */
    private void updateCategoryLevels(int base_level, CategoryAnnotation category){
        base_level ++;
        category.level = base_level;
        
        if(category.children != null && !category.children.isEmpty()){
            for(CategoryAnnotation category_child:category.children){
                updateCategoryLevels(base_level, category_child);
            }
        }
    }
}
