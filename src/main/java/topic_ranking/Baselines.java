package topic_ranking;

import entities.metadata.DBPediaAnnotation;
import ned.NERUtils;
import sparql_utils.annotation.ExtractCategoryAnnotations;
import utils_lod.FileUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Created by besnik on 09/06/2014.
 */
public class Baselines {

	private final static Logger LOGGER = Logger.getLogger(Baselines.class.getName());

    public void generateBaselineTopicRankings(Map<String, String> props, Set<String> datasetnames) {
        String baseline_type = props.get("hasBaseline");
        String baselines_path = props.get("termspath");
        String baselines_output = props.get("baseline_output");

        AccuracyComputation ac = new AccuracyComputation();
        if (baseline_type.equals("tfidf")) {
            for (String dataset : datasetnames) {
	            LOGGER.info("Generating baseline topic ranking for: " + baseline_type + " and for dataset " + dataset + " for " + 0 + " terms");
                String baseline_terms = baselines_path + dataset + "_tfidf.obj";
                String baseline_annotations = baselines_path + dataset + "_tfidf_dpbedia_500.obj";
                ac.generateBaselineRanking(baseline_type, baseline_terms, baseline_annotations, dataset, 0, baselines_output);
            }
        } else if (baseline_type.equals("lda")) {
            for (int topics = 10; topics <= 50; topics += 10) {
                for (int k = 50; k <= 200; k += 50) {
                    for (String dataset : datasetnames) {
                        //semantic-web-dog-food_lda_dpbedia_40_200.obj
	                    LOGGER.info("Generating baseline topic ranking for: " + baseline_type + " and for dataset " + dataset + " for " + topics + " topics and for " + k + " terms");
                        String baseline_terms = baselines_path + dataset + "_lda_" + topics + "_" + k + ".obj";
                        String baseline_annotations = baselines_path + dataset + "_lda_dpbedia_" + topics + "_" + k + ".obj";

	                    LOGGER.info(baseline_annotations);
                        ac.generateBaselineRanking(baseline_type, baseline_terms, baseline_annotations, dataset, k, baselines_output);
                    }
                }
            }
        }
    }

    public void baselineAnnotation(Map<String, String> props, Set<String> datasetnames) {
        ExtractCategoryAnnotations eca = new ExtractCategoryAnnotations();
        for (String dataset : datasetnames) {
            String hasBaseline = props.get("hasBaseline");
            if (hasBaseline.equals("tfidf")) {
                NERUtils ned = new NERUtils(props);
                Integer topicterms = Integer.parseInt(props.get("topicterms"));
                Map<String, Double> tfidfterms = (Map<String, Double>) FileUtils.readObject(props.get("termspath") + dataset + "_tfidf.obj");

                Map<Double, Set<String>> sortedtfidfterms = new TreeMap<Double, Set<String>>();
                for (String term : tfidfterms.keySet()) {
                    Set<String> subsortedtfidfterms = sortedtfidfterms.get(tfidfterms.get(term));
                    subsortedtfidfterms = subsortedtfidfterms == null ? new HashSet<String>() : subsortedtfidfterms;
                    sortedtfidfterms.put(tfidfterms.get(term), subsortedtfidfterms);

                    subsortedtfidfterms.add(term);
                }

                Set<String> terms = new HashSet<String>();
                int counter = 0;
                boolean stopAddingTerms = false;
                for (Double tfidfvalue : sortedtfidfterms.keySet()) {
                    if (stopAddingTerms) {
                        break;
                    }
                    Set<String> subsortedtfidfterms = sortedtfidfterms.get(tfidfvalue);
                    for (String term : subsortedtfidfterms) {
                        if (counter >= topicterms) {
                            stopAddingTerms = true;
                            break;
                        }

                        terms.add(term);
                        counter++;
                    }
                }
                Set<DBPediaAnnotation> tfidfannotations = new HashSet<DBPediaAnnotation>();

                for (String term : terms) {
                    Map<String, String> subdbpannotations = ned.performSimpleTagMeNER(term);
                    for (String surface_from : subdbpannotations.keySet()) {
                        DBPediaAnnotation dbp = new DBPediaAnnotation(subdbpannotations.get(surface_from));
                        dbp.uri = subdbpannotations.get(surface_from);
                        dbp.surfacefrom = surface_from;
                        dbp.category = eca.getCategories(dbp.uri, Long.valueOf(props.get("timeout")), props.get("dbpedia_url"));
                        tfidfannotations.add(dbp);
                    }
                }

                FileUtils.saveObject(tfidfannotations, props.get("termspath") + dataset + "_tfidf_dpbedia_" + topicterms + ".obj");
            }
            if (hasBaseline.equals("lda")) {
                NERUtils ned = new NERUtils(props);
                for (int topicno = 10; topicno <= 50; topicno += 10) {
                    for (int topicterms = 50; topicterms <= 200; topicterms += 50) {
                        Map<Integer, Set<Map.Entry<String, Double>>> ldatopics =
                                (Map<Integer, Set<Map.Entry<String, Double>>>) FileUtils.readObject(props.get("termspath") + dataset + "_lda_" + topicno + "_" + topicterms + ".obj");

                        Map<Integer, Set<DBPediaAnnotation>> topicannotations = new TreeMap<Integer, Set<DBPediaAnnotation>>();
                        for (int topic : ldatopics.keySet()) {
                            Set<DBPediaAnnotation> sub_topic_annotations = new HashSet<>();
                            topicannotations.put(topic, sub_topic_annotations);

                            Set<Map.Entry<String, Double>> subldatopics = ldatopics.get(topic);
                            for (Map.Entry<String, Double> entry : subldatopics) {
                                Map<String, String> subdbpannotations = ned.performSimpleTagMeNER(entry.getKey());

                                for (String surface_from : subdbpannotations.keySet()) {
                                    DBPediaAnnotation dbp = new DBPediaAnnotation(subdbpannotations.get(surface_from));
                                    dbp.uri = subdbpannotations.get(surface_from);
                                    dbp.surfacefrom = surface_from;
                                    dbp.category = eca.getCategories(dbp.uri, Long.valueOf(props.get("timeout")), props.get("dbpedia_url"));
                                    sub_topic_annotations.add(dbp);
                                }
                            }
                        }

                        FileUtils.saveObject(topicannotations, props.get("termspath") + dataset + "_lda_dpbedia_" + topicno + "_" + topicterms + ".obj");
                    }
                }
            }
        }
    }
}
