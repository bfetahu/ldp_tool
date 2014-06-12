/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package topic_ranking;

import entities.graph.GraphNode;
import entities.linkeddata.Dataset;
import entities.linkeddata.Resource;
import entities.metadata.CategoryAnnotation;
import entities.metadata.DBPediaAnnotation;
import sparql_utils.annotation.ExtractCategoryAnnotations;
import utils_lod.FileUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;

/**
 * @author besnik
 */
public class DatasetAnnotationGraphUtils {

	private final static Logger LOGGER = Logger.getLogger(DatasetAnnotationGraphUtils.class.getName());

    private Map<String, String> props;

    public DatasetAnnotationGraphUtils(Map<String, String> props) {
        this.props = props;
    }

    /**
     * Construct the dataset topic graph for ranking the profiles.
     */
    public void constructDatasetTopicGraph() {
        String dbpedia_url_tmp = props.get("dbpedia_endpoint");
        String[] dbpedia_tmp = dbpedia_url_tmp.split(",");
        Map<String, String> dbpedia_url = new TreeMap<String, String>();
        for (String line : dbpedia_tmp) {
            String[] data = line.split("\t");
            dbpedia_url.put(data[0].trim(), data[1].trim());
        }

        boolean includeEntities = props.get("includeEntities").equals("true");
        boolean load_entity_categories = props.get("load_entity_categories").equals("true");

        Set<String> datasetpaths = new HashSet<String>();
        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);

        Map<String, Map<String, Set<GraphNode>>> datasetgraph = null;
        Map<String, DBPediaAnnotation> dbpconcepts = (Map<String, DBPediaAnnotation>) FileUtils.readObject(props.get("annotationindex"));

        datasetgraph = getDatasetAnnotationGraph(datasetpaths, includeEntities, dbpconcepts, dbpedia_url, 1000, load_entity_categories);

        CategoryGraph cg = new CategoryGraph();
        cg.addDataGraph(datasetgraph);

	    LOGGER.info(" Vertices: " + cg.getVertexCount() + "\t Edges: " + cg.getEdgeCount());
        FileUtils.saveObject(cg, props.get("dataset_topic_graph"));
    }

    /**
     * Ranks the topics from the dataset-topic graph, using several graphical models.
     */
    public void rankTopics(Set<String> dataset_paths) {
        //sampling strategies for which we need to generate the corresponding graphs.
        String[] ranking_strategies = props.get("topic_ranking_strategies").split(",");
        Set<String> ranking_types = new HashSet<String>();
        ranking_types.addAll(Arrays.asList(ranking_strategies));

        int ranking_iterations = Integer.parseInt(props.get("ranking_iterations"));
        double alpha = Double.parseDouble(props.get("alpha"));
        int k_steps = Integer.parseInt(props.get("k_steps"));

        CategoryGraph cg = (CategoryGraph) FileUtils.readObject(props.get("dataset_topic_graph"));

        //get the corresponding topics and datasets resources
        Set<String> topics = new HashSet<String>();
        for (String node : cg.getVertices()) {
            if (node.contains("Category:")) {
                topics.add(node);
            }
        }
        Map<String, Set<String>> dataset_resources = new HashMap<>();
        for (String dataset_path : dataset_paths) {
            Dataset dataset = (Dataset) FileUtils.readObject(dataset_path);
            if (dataset == null)
                continue;
            Set<String> sub_resources = dataset.resources.keySet();
            dataset_resources.put(dataset.name, sub_resources);
        }

        //consider the given ranking types by generating the corresponding weights.
        for (String ranking_type : ranking_types) {
            if (ranking_type.equals("prank")) {
                //compute global ranking
                long time = System.nanoTime();
                String timestr = "";
                StringBuilder sb = new StringBuilder();

	            LOGGER.info("[Profiles]: PageRank with priors for " + props.get("sample_size") + "% of resources.");
                //compute global ranking
                if (!FileUtils.fileExists(props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj", false)) {
                    Map<String, Map<String, Double>> global_weights = cg.computeDatasetPageRankWithPriors(dataset_resources, alpha, ranking_iterations);
                    timestr = utils_lod.TimeUtils.measureComputingTime(time);
                    sb.append("[Profiles]\t").append(ranking_type).append("\tPercentage: ").append(props.get("sample_size")).append("%\tTime: ").append(timestr).append("\n");

                    FileUtils.saveObject(global_weights, props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj");
                    FileUtils.saveText(sb.toString(), props.get("topic_ranking_objects") + "PerformanceTime.txt", true);
                }

                sb = new StringBuilder();
	            LOGGER.info("[Topics]: PageRank with priors for " + props.get("sample_size") + "% of resources.");
            } else if (ranking_type.equals("hits")) {
	            LOGGER.info("[Dataset]: HITS with priors for " + props.get("sample_size") + "% of resources.");
                //compute global ranking
                long time = System.nanoTime();
                String timestr = "";
                StringBuilder sb = new StringBuilder();

                if (!FileUtils.fileExists(props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj", false)) {
                    Map<String, Map<String, Double>> global_weights = cg.computeDatasetHITSWithPriors(dataset_resources, alpha, ranking_iterations);
                    timestr = utils_lod.TimeUtils.measureComputingTime(time);
                    sb.append("[Dataset]\t").append(ranking_type).append("\tPercentage: ").append(props.get("sample_size")).append("%\tTime: ").append(timestr).append("\n");

                    FileUtils.saveObject(global_weights, props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj");
                    FileUtils.saveText(sb.toString(), props.get("topic_ranking_objects") + "PerformanceTime.txt", true);
                }

                sb = new StringBuilder();
	            LOGGER.info("[Topics]: HITS with priors for " + props.get("sample_size") + "% of resources.");
            } else if (ranking_type.equals("kstep")) {
                LOGGER.info("[Dataset]: K-Step Markov with priors for " + props.get("sample_size") + "% of resources.");
                //compute global ranking
                long time = System.nanoTime();
                String timestr = "";
                StringBuilder sb = new StringBuilder();

                if (!FileUtils.fileExists(props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj", false)) {
                    Map<String, Map<String, Double>> global_weights = cg.computeDatasetKStepMarkovWithPriors(dataset_resources, alpha, ranking_iterations, k_steps);
                    timestr = utils_lod.TimeUtils.measureComputingTime(time);
                    sb.append("[Dataset]\t").append(ranking_type).append("\tPercentage: ").append(props.get("sample_size")).append("%\tTime: ").append(timestr).append("\n");

                    FileUtils.saveObject(global_weights, props.get("topic_ranking_objects") + "Global_Ranking_" + ranking_type + "_" + props.get("sampling_type") + "_" + props.get("sample_size") + ".obj");
                    FileUtils.saveText(sb.toString(), props.get("topic_ranking_objects") + "PerformanceTime.txt", true);
                }
                sb = new StringBuilder();

                LOGGER.info("[Topics]: K-Step Markov with priors for " + props.get("sample_size") + "% of resources.");
            }
        }
    }


    /**
     * Filter out noisy topics and topics that conflict the hierarchy.
     */
    public void processAnnotationIndex() {
        String dbpedia_url_tmp = props.get("dbpedia_endpoint");
        String[] dbpedia_tmp = dbpedia_url_tmp.split(",");
        Map<String, String> dbpedia_url = new TreeMap<String, String>();
        for (String line : dbpedia_tmp) {
            String[] data = line.split("\t");
            dbpedia_url.put(data[0].trim(), data[1].trim());
        }

        Map<String, DBPediaAnnotation> dbpconcepts = (Map<String, DBPediaAnnotation>) FileUtils.readObject(props.get("annotationindex"));
        boolean load_entity_categories = props.get("load_entity_categories").equals("true");
        if (load_entity_categories) {
            long timeout = 1000;

            for (String entity_uri : dbpconcepts.keySet()) {
                loadEntitiesIndex(dbpedia_url, timeout, dbpconcepts.get(entity_uri), dbpconcepts);
            }
            FileUtils.saveObject(dbpconcepts, props.get("annotationindex"));
        }

        Map<String, Map<String, Double>> entity_category_scores = (Map<String, Map<String, Double>>) FileUtils.readObject(props.get("entity_category_scores"));

        for (String entityuri : dbpconcepts.keySet()) {
            DBPediaAnnotation dbpconcept = dbpconcepts.get(entityuri);
            Map<String, Double> category_scores = entity_category_scores.get(entityuri);
            if (category_scores == null || category_scores.size() == 1) {
                continue;
            }

            //check which categories represent temporal aspects i.e. 1950_establishments
            Map<String, Boolean> temporal_categories = new HashMap<String, Boolean>();
            for (String category : category_scores.keySet()) {
                boolean isTemporal = category.matches("[0-9]*\\_") || category.matches("\\_[0-9*]");
                temporal_categories.put(category, isTemporal);
            }

            //compute the average score from the non-temporal categories
            double average = 0;
            int count = 0;
            for (String category : category_scores.keySet()) {
                if (!temporal_categories.get(category)) {
                    average += category_scores.get(category);
                    count++;
                }
            }
            average /= count;

            //remove temporal categories and those below the average category score
            CategoryAnnotation main_category = dbpconcept.category;

            String entity_name = dbpconcept.uri.substring(dbpconcept.uri.lastIndexOf("/") + 1);
            if (main_category != null && main_category.children != null && !main_category.children.isEmpty()) {
                Iterator<CategoryAnnotation> category_iterator = main_category.children.iterator();
                while (category_iterator.hasNext()) {
                    CategoryAnnotation category = category_iterator.next();
                    String category_name = category.categoryname.substring(category.categoryname.lastIndexOf(":") + 1);

                    if (category_scores.get(category.categoryname) != null) {
                        if (category_name.equals(entity_name)) {
                            continue;
                        } else if (temporal_categories.get(category.categoryname) || category_scores.get(category.categoryname) < average) {
                            category_iterator.remove();
                            LOGGER.info(entityuri + "\t" + category.categoryname + "\tScore[" + category_scores.get(category.categoryname) + "]\t Avg[" + average + "]\t removed");
                        } else if (isContainedCategoryTree(category, main_category.children.iterator())) {
                            category_iterator.remove();
                            LOGGER.info(entityuri + "\t" + category.categoryname + "\tScore[" + category_scores.get(category.categoryname) + "]\t Avg[" + average + "]\t removed");
                        }
                    }
                }
            }
        }
        FileUtils.saveObject(dbpconcepts, props.get("annotationindex") + "_filtered");
    }

    /**
     * Computes the Normalised Topic Relevance Score per dataset for each of the categories associated with a dataset profile.
     */
    public void computeNormalisedScores() {
        NormalisationScore ns = new NormalisationScore();

        Set<String> datasetpaths = new HashSet<String>();
        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);

        Map<String, DBPediaAnnotation> dbpconcepts = (Map<String, DBPediaAnnotation>) FileUtils.readObject(props.get("annotationindex"));

        Map<String, Map<String, Entry<Double, Double>>> normalised_category_scores = ns.generateNormalisedScore(datasetpaths, dbpconcepts);
        FileUtils.saveObject(normalised_category_scores, props.get("normalised_topic_score"));
    }

    /**
     * Loads information about an entity if its not already loaded in the
     * entities index.
     *
     * @param dbpedia_url
     * @param timeout
     * @param dbpa
     * @param dbpconcepts
     */
    private DBPediaAnnotation loadEntitiesIndex(Map<String, String> dbpedia_url, long timeout, DBPediaAnnotation dbpa, Map<String, DBPediaAnnotation> dbpconcepts) {
        if (dbpconcepts.containsKey(dbpa.uri) && dbpconcepts.get(dbpa.uri).category != null && dbpconcepts.get(dbpa.uri).category.children != null && !dbpconcepts.get(dbpa.uri).category.children.isEmpty()) {
            return dbpconcepts.get(dbpa.uri);
        }
        ExtractCategoryAnnotations eca = new ExtractCategoryAnnotations();

	    LOGGER.info("Loading category information for entity: " + dbpa.uri);
        String lang_uri = dbpa.uri.substring(dbpa.uri.indexOf("http://") + "http://".length(), dbpa.uri.indexOf("dbpedia.org"));
        lang_uri = lang_uri.contains(".") ? lang_uri.substring(0, lang_uri.indexOf(".")) : lang_uri;
        lang_uri = lang_uri.isEmpty() ? "en" : lang_uri;

        dbpa.category = eca.getCategories(dbpa.uri, timeout, dbpedia_url.get(lang_uri));
        dbpconcepts.put(dbpa.uri, dbpa);

        return dbpa;
    }


    /**
     * Generates the data graph of datasets, resources and the corresponding
     * categories/entities. A category is directly associated with the resources
     * from which it is extracted, consequently a resource is associated with a
     * dataset to whom it belongs.
     *
     * @param datasetpaths
     * @param includeEntities
     */
    private Map<String, Map<String, Set<GraphNode>>> getDatasetAnnotationGraph(Set<String> datasetpaths, boolean includeEntities, Map<String, DBPediaAnnotation> dbpconcepts,
                                                                               Map<String, String> dbpedia_url, long timeout, boolean load_entity_categories) {
        //normalised topics scores
        Map<String, Map<String, Entry<Double, Double>>> normalised_category_scores = (Map<String, Map<String, Entry<Double, Double>>>) FileUtils.readObject(props.get("normalised_topic_score"));
        double mean_ntr = getMeanTopicNormalisedScore(normalised_category_scores);
        //the index of generated nodes
        Map<String, GraphNode> nodes = new TreeMap<String, GraphNode>();

        //the generated dataset, resource, and annotation graph.
        Map<String, Map<String, Set<GraphNode>>> datasetgraph = new TreeMap<String, Map<String, Set<GraphNode>>>();

        //for each dataset load the annotated data into the graph.
        for (String datasetpath : datasetpaths) {
            Dataset dataset = (Dataset) FileUtils.readObject(datasetpath);
            if (dataset == null || FileUtils.fileExists(props.get("raw_graph_dir") + "/Graph_raw_" + dataset.name + ".obj", true)) {
                continue;
            }
	        LOGGER.info("Processing dataset: " + dataset.name);

            //generate the subgraph of the dataset.
            Map<String, Set<GraphNode>> subdatasetgraph = datasetgraph.get(dataset.name);
            subdatasetgraph = subdatasetgraph == null ? new TreeMap<String, Set<GraphNode>>() : subdatasetgraph;
            datasetgraph.put(dataset.name, subdatasetgraph);

            for (String resourceuri : dataset.resources.keySet()) {
                Resource resource = dataset.resources.get(resourceuri);
                if (!resource.hasAnnotations() || resourceuri.contains("dbpedia.org")) {
                    continue;
                }
                addNodesToGraph(subdatasetgraph, resource, load_entity_categories, dbpedia_url, timeout, dbpconcepts, includeEntities, nodes, dataset.name, normalised_category_scores, mean_ntr);
            }

            FileUtils.saveObject(subdatasetgraph, props.get("raw_graph_dir") + "/Graph_raw_" + dataset.name + ".obj");
        }
        return datasetgraph;
    }

    /**
     * Returns the mean normalised topic relevance score, used for filtering out noisy topics.
     * @param normalised_category_scores
     * @return
     */
    private double getMeanTopicNormalisedScore(Map<String, Map<String, Entry<Double, Double>>> normalised_category_scores){
        double avg = 0;
        int count = 0;
        for(String category:normalised_category_scores.keySet()){
            for(String dataset:normalised_category_scores.get(category).keySet()){
                Entry<Double,Double> category_entry = normalised_category_scores.get(category).get(dataset);
                avg += (category_entry.getKey() + category_entry.getValue());
                count ++;
            }
        }
        return avg / count;
    }

    /**
     * Add entities and categories from each of the resources.
     *
     * @param subdatasetgraph
     * @param resource
     * @param load_entity_categories
     * @param dbpedia_url
     * @param timeout
     * @param dbpconcepts
     * @param includeEntities
     * @param nodes
     * @param dataset_name
     */
    private void addNodesToGraph(Map<String, Set<GraphNode>> subdatasetgraph, Resource resource, boolean load_entity_categories,
                                 Map<String, String> dbpedia_url, long timeout, Map<String, DBPediaAnnotation> dbpconcepts, boolean includeEntities,
                                 Map<String, GraphNode> nodes, String dataset_name, Map<String, Map<String, Entry<Double, Double>>> normalised_category_scores, double mean_ntr) {
        Set<GraphNode> resourcenodes = subdatasetgraph.get(resource.id);
        resourcenodes = resourcenodes == null ? new HashSet<GraphNode>() : resourcenodes;
        subdatasetgraph.put(resource.id, resourcenodes);

        if (!resource.hasAnnotations()) {
            return;
        }

        for (String entity_surface : resource.annotations.keySet()) {
            String entity_uri = resource.annotations.get(entity_surface);
            String lang_uri = entity_uri.substring(entity_uri.indexOf("http://") + "http://".length(), entity_uri.indexOf("dbpedia.org"));
            lang_uri = lang_uri.contains(".") ? lang_uri.substring(0, lang_uri.indexOf(".")) : "";
            DBPediaAnnotation dbpa = dbpconcepts.get(resource.annotations.get(entity_uri));

            if (lang_uri.trim().isEmpty()) {
                dbpa = dbpconcepts.get(entity_uri);
            }

            if (load_entity_categories) {
                dbpa = loadEntitiesIndex(dbpedia_url, timeout, dbpa, dbpconcepts);
            }

            if (dbpa == null) {
                continue;
            }

            if (includeEntities) {
                GraphNode node = nodes.get(dbpa.uri);
                node = node == null ? new GraphNode() : node;
                nodes.put(dbpa.uri, node);

                //add edge between resource and node.
                Entry<String, Double> edge = new AbstractMap.SimpleEntry<String, Double>(dataset_name, 0.0);
                node.edge_weights.add(edge);
                node.node_uri = dbpa.uri;

                resourcenodes.add(node);
            }

            //add the categories into the data-graph.
            addChildCategoriesToGraph(resource.id, dbpa.category, resourcenodes, dataset_name, nodes, normalised_category_scores, mean_ntr);
        }
    }

    /**
     * Add recursively the child nodes of a category, by assigning weights to
     * the respective resource URI. This can be configured to establish weights
     * with a dataset rather than a resource, depending on the required level of
     * detail.
     *
     * @param resourceuri
     * @param cat
     * @param subdatasetgraph
     * @param nodes
     */
    private void addChildCategoriesToGraph(String resourceuri, CategoryAnnotation cat, Set<GraphNode> subdatasetgraph, String dataset_name,
                                           Map<String, GraphNode> nodes, Map<String, Map<String, Entry<Double, Double>>> normalised_category_scores, double mean_ntr) {
        if (cat == null || cat.categoryname == null) {
            return;
        }

        Map<String, Entry<Double, Double>> category_score = normalised_category_scores.get(cat.categoryname);
        if (category_score != null && category_score.containsKey(dataset_name)) {
            double cat_ntr = category_score.get(dataset_name).getKey() + category_score.get(dataset_name).getValue();
            if(cat_ntr > mean_ntr){
                return;
            }
        }

        GraphNode node = nodes.get(cat.categoryname);
        node = node == null ? new GraphNode() : node;
        nodes.put(cat.categoryname, node);

        node.node_uri = cat.categoryname;
        node.level = cat.level;
        node.isCategory = true;

        //add edge between resource and node.
        Entry<String, Double> edge = new AbstractMap.SimpleEntry<String, Double>(resourceuri, 0.0);
        node.edge_weights.add(edge);

        subdatasetgraph.add(node);

        //check if it has children
        if (cat.children != null && !cat.children.isEmpty()) {
            for (CategoryAnnotation catchild : cat.children) {
                addChildCategoriesToGraph(resourceuri, catchild, subdatasetgraph, dataset_name, nodes, normalised_category_scores, mean_ntr);
            }
        }
    }

    /**
     * Checks if a category is contained within the category tree of another
     * category.
     *
     * @param category
     * @param categories
     * @return
     */
    private static boolean isContainedCategoryTree(CategoryAnnotation category, Iterator<CategoryAnnotation> categories) {
        while (categories.hasNext()) {
            CategoryAnnotation category_cmp = categories.next();
            if (category_cmp.categoryname.equals(category.categoryname)) {
                continue;
            }

            if (category_cmp.containsChild(category.categoryname)) {
                return true;
            }
        }

        return false;
    }
}
