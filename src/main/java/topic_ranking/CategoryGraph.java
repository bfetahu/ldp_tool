package topic_ranking;

import edu.uci.ics.jung.algorithms.scoring.HITS;
import edu.uci.ics.jung.algorithms.scoring.HITSWithPriors;
import edu.uci.ics.jung.algorithms.scoring.KStepMarkov;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import entities.graph.GraphNode;
import org.apache.commons.collections15.Transformer;
import org.apache.commons.collections15.TransformerUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * CategoryGraph - constructs the graph of all categories extracted from the
 * entities. It additionally computes several graph metrics such as PageRank,
 * Centrality etc.
 * <p>
 * This class also provides an interface on computing the importance of pre/post
 * filtered subgraphs.
 *
 * @author besnik
 */
public class CategoryGraph extends DirectedSparseGraph<String, String> implements Serializable {

    private static final long serialVersionUID = -4204251981433069308L;
    private static final Logger LOGGER = Logger.getLogger(CategoryGraph.class.getName());

    /**
     * @param datagraph
     */
    public void addDataGraph(Map<String, Map<String, Set<GraphNode>>> datagraph) {
        //for each dataset add their resource instances
        for (String dataset : datagraph.keySet()) {
            //add dataset node.
            addVertex(dataset);

            //for each resource instance of the dataset add the corresponding vertices
            Map<String, Set<GraphNode>> subdatagraph = datagraph.get(dataset);
            for (String resource : subdatagraph.keySet()) {
                //add resource node
                addVertex(resource);
                //add the edge between the resource and dataset
                String edge_label = dataset + "\t" + resource;
                addEdge(edge_label, dataset, resource, EdgeType.DIRECTED);

                Set<GraphNode> nodes = subdatagraph.get(resource);
                //for each resource add the topics that are assigned to them.
                for (GraphNode node : nodes) {
                    //add the topic node
                    if (node == null || node.node_uri == null || node.node_uri.isEmpty()) {
                        continue;
                    }

                    addVertex(node.node_uri);

                    //add the edge between the topic and the resource to which is assigned
                    String topic_edge_label = resource + "\t" + node.node_uri;
                    addEdge(topic_edge_label, resource, node.node_uri, EdgeType.DIRECTED);
                }
            }
        }
    }

    /**
     * Computes the PageRank with priors by considering each of the datasets
     * individually as prior knowledge. The resulting data structure is returned
     * as <dataset, <node, score>> computed for each dataset.
     *
     * @param datasets
     * @param alpha
     * @param ranking_iterations
     * @return
     */
    public Map<String, Map<String, Double>> computeDatasetPageRankWithPriors(Map<String, Set<String>> datasets, double alpha, int ranking_iterations) {
        //consider each dataset as prior knowledge and compute the corresponding PagRank values.
        //store the weights by computing PageRank considering each dataset as prior knowledge.
        Map<String, Map<String, Double>> weights = new TreeMap<String, Map<String, Double>>();
        //the set of vertices in the current data-graph.
        Collection<String> verticelist = getVertices();

        for (String dataset : datasets.keySet()) {
            if (datasets.get(dataset).isEmpty()) {
                continue;
            }

            Map<String, Double> priors = new TreeMap<String, Double>();
            //set the priors for the rest of vertices to zero.
            for (String vertex : verticelist) {
                priors.put(vertex, 0.0);
            }

            Set<String> dataset_resources = datasets.get(dataset);
            double prior_probabilities = 1 / (double) (dataset_resources.size() + 1);
            for (String resource : dataset_resources) {
                priors.put(resource, prior_probabilities);
            }
            priors.put(dataset, prior_probabilities);

            //capture the weights for the dataset
            Map<String, Double> sub_weights = weights.get(dataset);
            sub_weights = sub_weights == null ? new TreeMap<String, Double>() : sub_weights;
            weights.put(dataset, sub_weights);

            //transform the map of prior values into suitable format
            Transformer<String, Double> transformer = TransformerUtils.mapTransformer(priors);
            PageRankWithPriors<String, String> pr = new PageRankWithPriors<String, String>(this, transformer, alpha);
            pr.acceptDisconnectedGraph(true);
            pr.setMaxIterations(ranking_iterations);
            pr.initialize();
            pr.evaluate();

            for (String vertex : verticelist) {
                sub_weights.put(vertex, pr.getVertexScore(vertex));
            }
        }
        return weights;
    }

    /**
     * Computes the HITS with priors by considering each dataset as prior
     * knowledge. Furthermore, the authorities and hubs for HITS are considered
     * to be equal, hence their values will be the same. The set of computed
     * weights is returned as the datastructure <dataset, <node, score>>
     *
     * @param datasets
     * @param alpha
     * @param ranking_iterations
     * @return
     */
    public Map<String, Map<String, Double>> computeDatasetHITSWithPriors(Map<String, Set<String>> datasets, double alpha, int ranking_iterations) {
        //store the weights by computing HITS considering each dataset as prior knowledge.
        Map<String, Map<String, Double>> weights = new TreeMap<String, Map<String, Double>>();
        //the set of vertices in the current data-graph.
        Collection<String> verticelist = getVertices();

        for (String dataset : datasets.keySet()) {
            if (datasets.get(dataset).isEmpty())
                continue;

            Map<String, HITS.Scores> priors = new TreeMap<String, HITS.Scores>();
            //set the priors for the rest of vertices to zero.
            HITS.Scores score_zero = new HITS.Scores(0.0, 0.0);
            for (String vertex : verticelist) {
                priors.put(vertex, score_zero);
            }

            Set<String> dataset_resources = datasets.get(dataset);
            double prior_probabilities = 1 / (double) (dataset_resources.size() + 1);
            HITS.Scores score = new HITS.Scores(prior_probabilities, prior_probabilities);

            for (String resource : dataset_resources) {
                priors.put(resource, score);
            }
            priors.put(dataset, score);

            //capture the weights for the dataset
            Map<String, Double> sub_weights = weights.get(dataset);
            sub_weights = sub_weights == null ? new TreeMap<String, Double>() : sub_weights;
            weights.put(dataset, sub_weights);

            Transformer<String, HITS.Scores> transformer = TransformerUtils.mapTransformer(priors);

            HITSWithPriors<String, String> hits = new HITSWithPriors<String, String>(this, transformer, alpha);
            hits.setMaxIterations(ranking_iterations);
            hits.acceptDisconnectedGraph(true);
            hits.initialize();
            hits.evaluate();

            for (String vertex : verticelist) {
                sub_weights.put(vertex, hits.getVertexScore(vertex).authority);
            }
        }

        return weights;
    }

    /**
     * Computes the K-Step Markov with priors by considering each dataset as
     * prior knowledge.
     *
     * @param alpha
     * @param ranking_iterations
     * @param steps
     * @return
     */
    public Map<String, Map<String, Double>> computeDatasetKStepMarkovWithPriors(Map<String, Set<String>> datasets, double alpha, int ranking_iterations, int steps) {
        //store the weights by computing K-Step Markov considering each dataset as prior knowledge.
        Map<String, Map<String, Double>> weights = new TreeMap<String, Map<String, Double>>();
        //the set of vertices in the current data-graph.
        Collection<String> verticelist = getVertices();

        for (String dataset : datasets.keySet()) {
            LOGGER.info("Processing for dataset: " + dataset);
            if (datasets.get(dataset).isEmpty()) {
                continue;
            }

            Map<String, Double> priors = new TreeMap<String, Double>();
            for (String vertex : verticelist) {
                priors.put(vertex, 0.0);
            }

            Set<String> dataset_resources = datasets.get(dataset);
            double prior_probabilities = 1 / (double) (dataset_resources.size() + 1);
            for (String resource : dataset_resources) {
                priors.put(resource, prior_probabilities);
            }
            priors.put(dataset, prior_probabilities);

            //capture the weights for the dataset
            Map<String, Double> sub_weights = weights.get(dataset);
            sub_weights = sub_weights == null ? new TreeMap<String, Double>() : sub_weights;
            weights.put(dataset, sub_weights);

            //transform the map of prior values into suitable format
            Transformer<String, Double> transformer = TransformerUtils.mapTransformer(priors);
            KStepMarkov<String, String> km = new KStepMarkov<String, String>(this, transformer, steps);
            km.acceptDisconnectedGraph(true);
            km.setMaxIterations(ranking_iterations);
            km.initialize();
            km.evaluate();

            for (String vertex : verticelist) {
                sub_weights.put(vertex, km.getVertexScore(vertex));
            }
        }
        return weights;
    }
}
