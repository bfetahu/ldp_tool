/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset_exporter;

import entities.linkeddata.Dataset;
import entities.linkeddata.Resource;
import entities.metadata.CategoryAnnotation;
import entities.metadata.DBPediaAnnotation;
import utils_lod.FileUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;

/**
 * @author besnik
 */
public class DatasetExporter {

	private final static Logger LOGGER = Logger.getLogger(DatasetExporter.class.getClass().getName());

    /**
     * Get the JSON representation of the datasets along with their generated
     * profiles.
     *
     * @param datasetspath
     * @param ranking_types
     * @param ranking_path
     * @param normalised_score_path
     */
    public static void getJSONRepresentationDatasets(Set<String> datasetspath, String outdir, Set<String> ranking_types, String sampling_type, String sample_size, String ranking_path,
                                                     String normalised_score_path, String annotation_index_path) {
        //load annotations index
        Map<String, DBPediaAnnotation> annotation_index = (Map<String, DBPediaAnnotation>) FileUtils.readObject(annotation_index_path);

        StringBuilder sb = new StringBuilder();

        //normalised score
        Map<String, Map<String, Entry<Double, Double>>> normalised_score = (Map<String, Map<String, Entry<Double, Double>>>) FileUtils.readObject(normalised_score_path);
        //generate the dataset, category, rank scores
        Map<String, Map<String, Double>> dataset_normalised_scores = new TreeMap<String, Map<String, Double>>();
        for (String category : normalised_score.keySet()) {
            for (String dataset : normalised_score.get(category).keySet()) {
                Entry<Double, Double> entry = normalised_score.get(category).get(dataset);

                Map<String, Double> sub_dataset_normalised_scores = dataset_normalised_scores.get(dataset);
                sub_dataset_normalised_scores = sub_dataset_normalised_scores == null ? new TreeMap<String, Double>() : sub_dataset_normalised_scores;
                dataset_normalised_scores.put(dataset, sub_dataset_normalised_scores);

                sub_dataset_normalised_scores.put(category, entry.getKey() + entry.getValue());
            }
        }

        //global dataset profiles
        Map<String, Map<String, Map<String, Double>>> dataset_profiles = new TreeMap<String, Map<String, Map<String, Double>>>();

        //load the topic ranking for each dataset.
        for (String ranking_type : ranking_types) {
            if (ranking_type.equals("normalised")) {
                dataset_profiles.put(ranking_type, dataset_normalised_scores);
            } else {
                String ranking_file = ranking_path + "Global_Ranking_" + ranking_type + "_" + sampling_type + "_" + sample_size + ".obj";
                Map<String, Map<String, Double>> ranking_data = (Map<String, Map<String, Double>>) FileUtils.readObject(ranking_file);
                dataset_profiles.put(ranking_type, ranking_data);
            }
        }

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();

        String json_file_name = "DatasetProfiles_" + dateFormat.format(date) + ".json";
        //load each dataset
        int counter = 0;
        if (FileUtils.fileExists(outdir + "/" + json_file_name, true)) {
            counter++;
            sb.append(FileUtils.readText(outdir + json_file_name));
        } else {
            FileUtils.saveText("{\"Datasets\": [", outdir + "/" + json_file_name, true);
        }
        for (String datasetpath : datasetspath) {
            StringBuilder sb_tmp = new StringBuilder();

	        LOGGER.info("Printing JSON for dataset: " + datasetpath);
            if (counter != 0) {
                sb_tmp.append(",");
            }

            Dataset dataset = (Dataset) FileUtils.readObject(datasetpath);
            if (dataset == null || dataset.resources.isEmpty()) {
                continue;
            }

            counter++;
            Set<String> resource_indices = dataset.resources.keySet();
            sb_tmp.append(getDatasetJSONRepresentation(dataset, dataset_profiles, resource_indices, annotation_index));
            FileUtils.saveText(sb_tmp.toString(), outdir + json_file_name, true);
        }
        FileUtils.saveText("]}", outdir + "/" + json_file_name, true);
    }

    /**
     * Gets the JSON string representation of the dataset.
     *
     * @param dataset
     * @param datasets_profile
     * @return
     */
    public static String getDatasetJSONRepresentation(Dataset dataset, Map<String, Map<String, Map<String, Double>>> datasets_profile, Set<String> resource_indices,
                                                      Map<String, DBPediaAnnotation> annotation_index) {
        StringBuilder sb = new StringBuilder();

        //append the header of the dataset
        sb.append("{\n\t\"Dataset\": { ");
        sb.append("\n\t\t\"URI\": ").append("\"").append(dataset.url).append("\",");
        sb.append("\n\t\t\"Name\": ").append("\"").append(dataset.name).append("\",");
        sb.append("\n\t\t\"Description\": ").append("\"").append("").append("\",");

        //append dataset resource types analytics information.
        sb.append("\n\t\t\"Annotations\":{ ").append(getAnnotationJSONRepresentation(dataset, datasets_profile, annotation_index, resource_indices)).append("\n\t\t}");
        sb.append("\n\t\t}\n\t}");


        return sb.toString();
    }

    /**
     * Get the JSON String representation of the generated datasets profiles.
     *
     * @param dataset
     * @return
     */
    private static String getAnnotationJSONRepresentation(Dataset dataset, Map<String, Map<String, Map<String, Double>>> datasets_profile,
                                                          Map<String, DBPediaAnnotation> annotation_index, Set<String> resource_indices) {
        StringBuilder sb = new StringBuilder();

        //load the dataset profile topics.
	    LOGGER.info(dataset.name);

        // the entity and resource associations
        Map<String, Set<String>> entity_resources = new TreeMap<String, Set<String>>();
        //category and entity associations.
        Map<String, Set<String>> category_entities = new TreeMap<String, Set<String>>();

        if (resource_indices != null) {
	        LOGGER.info("Dataset has sampling indices");
            for (String resource_uri : resource_indices) {
                if (resource_uri.contains("dbpedia")) {
                    continue;
                }

                Resource resource = dataset.resources.get(resource_uri);
                if (resource.hasAnnotations()) {
                    for (String entity_uri : resource.annotations.keySet()) {
                        DBPediaAnnotation dbp_concept = annotation_index.get(resource.annotations.get(entity_uri));

                        Set<String> sub_entity_resources = entity_resources.get(resource.annotations.get(entity_uri));
                        sub_entity_resources = sub_entity_resources == null ? new HashSet<String>() : sub_entity_resources;
                        entity_resources.put(resource.annotations.get(entity_uri), sub_entity_resources);
                        sub_entity_resources.add(resource_uri);

                        //add the categories into the map data structure.
                        getCategoryEntityMap(resource.annotations.get(entity_uri), category_entities, dbp_concept.category);
                    }
                }
            }
        } else {
	        LOGGER.info("Dataset does not have sampling indices");
            for (String resource_uri : dataset.resources.keySet()) {
                if (resource_uri.contains("dbpedia")) {
                    continue;
                }

                Resource resource = dataset.resources.get(resource_uri);
                if (!resource.hasAnnotations()) {
                    continue;
                }

                for (String entity_uri : resource.annotations.keySet()) {
                    DBPediaAnnotation dbp_concept = annotation_index.get(resource.annotations.get(entity_uri));

                    Set<String> sub_entity_resources = entity_resources.get(entity_uri);
                    sub_entity_resources = sub_entity_resources == null ? new HashSet<String>() : sub_entity_resources;
                    entity_resources.put(resource.annotations.get(entity_uri), sub_entity_resources);
                    sub_entity_resources.add(resource_uri);

                    //add the categories into the map data structure.
                    getCategoryEntityMap(resource.annotations.get(entity_uri), category_entities, dbp_concept.category);
                }
            }
        }

        sb.append("\n\t\t\t\"Entities\": [").append(getEntitiesJSONRepresentation(entity_resources)).append("],\n");
        sb.append("\n\t\t\t\"Categories\": [").append(getCategoryJSONRepresentation(category_entities, datasets_profile, dataset.name)).append("]\n");
        return sb.toString();
    }

    /**
     * Returns the JSON representation of the entity and the resources it is
     * associated.
     *
     * @param entity_resources
     * @return
     */
    private static String getEntitiesJSONRepresentation(Map<String, Set<String>> entity_resources) {
        StringBuilder sb = new StringBuilder();
        int count_entities = 0;

        for (String entity : entity_resources.keySet()) {
            if (count_entities != 0) {
                sb.append(", ");
            }
            count_entities += 1;

            sb.append("\n\t\t\t\t{\n\t\t\t\t\t\"URI\": \"").append(entity).append("\", \n");
            sb.append("\n\t\t\t\t\t\"Resources\": [");

            int counter = 0;
            for (String resource : entity_resources.get(entity)) {
                if (counter != 0) {
                    sb.append(", ");
                }
                counter++;
                sb.append("{\"Resource\": \"").append(resource).append("\"}");
            }
            sb.append("]\n\t\t\t\t}");
        }

        return sb.toString();
    }

    /**
     * Gets the JSON category representation.
     *
     * @param category_entities
     * @return
     */
    private static String getCategoryJSONRepresentation(Map<String, Set<String>> category_entities, Map<String, Map<String, Map<String, Double>>> dataset_profiles, String dataset) {
        int entity_counter = 0;
        StringBuilder sb = new StringBuilder();
        for (String category : category_entities.keySet()) {
            if (category.equals("Main_Classification")) {
                continue;
            }

            if (entity_counter != 0) {
                sb.append(", ");
            }
            entity_counter += 1;
            sb.append("\n\t\t\t\t{\n\t\t\t\t\t\"URI\":\" ").append(category).append("\", \n");
            sb.append("\n\t\t\t\t\t\"Entities\":[");

            int counter = 0;
            for (String entity : category_entities.get(category)) {
                if (counter != 0) {
                    sb.append(", ");
                }
                counter++;
                sb.append("{\"Entity\": \"").append(entity).append("\"}");
            }
            sb.append("\n\t\t\t\t\t],\n");
            sb.append("\n\t\t\t\t\t\"Score\": [");

            int rank_count = 0;
            next_rank:
            for (String ranking_type : dataset_profiles.keySet()) {
                for (String dataset_id : dataset_profiles.get(ranking_type).keySet()) {
                    if (dataset_id.trim().equals(dataset)) {
                        if (rank_count != 0) {
                            sb.append(", ");
                        }

                        Double value = dataset_profiles.get(ranking_type).get(dataset_id).get(category);
                        value = value == null ? dataset_profiles.get(ranking_type).get(dataset_id).get(category + " ") : value;

                        sb.append("{\"name\":\"").append(ranking_type).append("\", \"value\": \"").append(value).append("\"}");

                        rank_count++;
                        continue next_rank;
                    }
                }
            }
            sb.append("\n\t\t\t\t\t],\n");
            sb.append("\n\t\t\t\t\t\"Frequency\": \"0\"\n\t\t\t\t}");
        }

        return sb.toString();
    }

    /**
     * Generates the category map data-structure with the corresponding
     * entities.
     *
     * @param entity_uri
     * @param categories
     * @param category
     */
    private static void getCategoryEntityMap(String entity_uri, Map<String, Set<String>> categories, CategoryAnnotation category) {
        if (category != null && category.categoryname != null) {
            Set<String> category_resources = categories.get(category.categoryname);
            category_resources = category_resources == null ? new HashSet<String>() : category_resources;
            categories.put(category.categoryname, category_resources);

            category_resources.add(entity_uri);

            if (category.children != null && !category.children.isEmpty()) {
                for (CategoryAnnotation catchild : category.children) {
                    getCategoryEntityMap(entity_uri, categories, catchild);
                }
            }
        }
    }
}
