/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package topic_ranking;

import entities.linkeddata.Dataset;
import entities.linkeddata.Resource;
import entities.metadata.CategoryAnnotation;
import entities.metadata.DBPediaAnnotation;
import utils_lod.FileUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;

/**
 *
 * @author besnik
 */
public class NormalisationScore {

	private final static Logger LOGGER = Logger.getLogger(NormalisationScore.class.getName());

    /**
     * Compute normalised score using the following formula: score(t,D) =
     * #entities(D)/#entities(t,D) + #entities/#entities(t) It represents a
     * variant of the TFxIDF score.
     *
     * @param datasetpaths
     * @return
     */
    public Map<String, Map<String, Entry<Double, Double>>> generateNormalisedScore(Set<String> datasetpaths, Map<String, DBPediaAnnotation> dbpconcepts) {
        Map<String, Map<String, Entry<Double, Double>>> category_normalised_scores = new TreeMap<String, Map<String, Entry<Double, Double>>>();
        Map<String, Set<String>> dataset_entities = new TreeMap<String, Set<String>>();

        Map<String, Map<String, Set<String>>> category_associations = getCategoriesForDatasets(datasetpaths, dataset_entities, dbpconcepts);

        int total_entities = 0;
        for (String datasetname : dataset_entities.keySet()) {
            total_entities += dataset_entities.get(datasetname).size();
        }

        Map<String, Integer> total_category_entities = new TreeMap<String, Integer>();
        for (String category_uri : category_associations.keySet()) {
            int category_entities_total = 0;
            for (String datasetname : category_associations.get(category_uri).keySet()) {
                category_entities_total += category_associations.get(category_uri).get(datasetname).size();
            }
            total_category_entities.put(category_uri, category_entities_total);
        }

        //generate the normalised score for each category and the corresponding dataset.
        for (String category_uri : category_associations.keySet()) {
            Map<String, Entry<Double, Double>> category_scores = category_normalised_scores.get(category_uri);
            category_scores = category_scores == null ? new HashMap<String, Entry<Double, Double>>() : category_scores;
            category_normalised_scores.put(category_uri, category_scores);

            Map<String, Set<String>> category_entity_associations = category_associations.get(category_uri);

            //for each dataset now compute the score.
            for (String datasetname : category_entity_associations.keySet()) {
                Set<String> cat_entities = category_entity_associations.get(datasetname);
                Set<String> sub_dataset_entities = dataset_entities.get(datasetname);

                if (cat_entities != null && sub_dataset_entities != null) {
                    double topic_significance = cat_entities.size() / (double) total_category_entities.get(category_uri);
                    double dataset_normalisation = total_entities / sub_dataset_entities.size();
                        
                    double score =  topic_significance + dataset_normalisation;
                    Entry<Double, Double> cat_entry = new AbstractMap.SimpleEntry<Double, Double>(topic_significance, score);
                    category_scores.put(datasetname, cat_entry);
                }
            }
        }

        return category_normalised_scores;
    }

    /**
     * Generate the normalised score for assigned categories for each datasets
     * by first generating the corresponding datastructures.
     *
     * @param datasetpaths
     */
    public Map<String, Map<String, Set<String>>> getCategoriesForDatasets(Set<String> datasetpaths, Map<String, Set<String>> dataset_entities, Map<String, DBPediaAnnotation> dbpconcepts) {
        //store the number of associations a category has within a dataset and from the different entities.
        Map<String, Map<String, Set<String>>> category_entity = new TreeMap<String, Map<String, Set<String>>>();

        for (String datasetpath : datasetpaths) {
            Dataset dataset = (Dataset) FileUtils.readObject(datasetpath);
            if (dataset == null) {
                continue;
            }

	        LOGGER.info("Processing dataset: " + dataset.name);

            //store the set of entities associated with a dataset.
            Set<String> sub_dataset_entities = dataset_entities.get(dataset.name);
            sub_dataset_entities = sub_dataset_entities == null ? new HashSet<String>() : sub_dataset_entities;
            dataset_entities.put(dataset.name, sub_dataset_entities);

            //iterate over all enrichments.
            for (String resourceuri : dataset.resources.keySet()) {
                Resource resource = dataset.resources.get(resourceuri);
                if (resource == null || !resource.hasAnnotations()) {
                    continue;
                }

                //check all the annotations for an entity and perform the corresponding analytics.
                if (!resource.hasAnnotations()) {
                    continue;
                }

                for (String dbpuri : resource.annotations.keySet()) {
                    DBPediaAnnotation dbp_concept = dbpconcepts.get(resource.annotations.get(dbpuri));
                    if (dbp_concept == null) {
                        continue;
                    }
                    //add entities for the specific dataset.
                    sub_dataset_entities.add(dbp_concept.uri);

                    if (dbp_concept.category != null && dbp_concept.category.categoryname != null) {
                        //store the set of categories associated with a dataset
                        Map<String, Set<String>> sub_category_entity = category_entity.get(dbp_concept.category.categoryname);
                        sub_category_entity = sub_category_entity == null ? new TreeMap<String, Set<String>>() : sub_category_entity;
                        category_entity.put(dbp_concept.category.categoryname, sub_category_entity);

                        //assign the following entity for the category and dataset
                        Set<String> dataset_entity_category_support = sub_category_entity.get(dataset.name);
                        dataset_entity_category_support = dataset_entity_category_support == null ? new HashSet<String>() : dataset_entity_category_support;
                        sub_category_entity.put(dataset.name, dataset_entity_category_support);
                        
                        dataset_entity_category_support.add(dbp_concept.uri);

                        //add the corresponding child categories as well.
                        if (dbp_concept.category.children != null && !dbp_concept.category.children.isEmpty()) {
                            for (CategoryAnnotation categorychild : dbp_concept.category.children) {
                                addChildCategoriesForNormalisationScore(category_entity, dataset.name, dbpuri, categorychild);
                            }
                        }
                    }
                }
            }
        }

        return category_entity;
    }

    /**
     * Add recursively the child categories to the corresponding datastructures
     * used for the normalisation score.
     *
     * @param category_entity
     * @param datasetname
     * @param entityuri
     * @param category
     */
    private void addChildCategoriesForNormalisationScore(Map<String, Map<String, Set<String>>> category_entity, String datasetname, String entityuri, CategoryAnnotation category) {
        if (category != null) {
            Map<String, Set<String>> sub_category_entity = category_entity.get(category.categoryname);
            sub_category_entity = sub_category_entity == null ? new TreeMap<String, Set<String>>() : sub_category_entity;
            category_entity.put(category.categoryname, sub_category_entity);

            //assign the following entity for the category and dataset
            Set<String> dataset_entity_category_support = sub_category_entity.get(datasetname);
            dataset_entity_category_support = dataset_entity_category_support == null ? new HashSet<String>() : dataset_entity_category_support;
            sub_category_entity.put(datasetname, dataset_entity_category_support);

            dataset_entity_category_support.add(entityuri);

            if (category.children != null && !category.children.isEmpty()) {
                for (CategoryAnnotation categorychild : category.children) {
                    addChildCategoriesForNormalisationScore(category_entity, datasetname, entityuri, categorychild);
                }
            }
        }
    }
}
