/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package topic_ranking;

import entities.metadata.CategoryAnnotation;
import entities.metadata.DBPediaAnnotation;
import utils_lod.FileUtils;

import java.util.*;
import java.util.Map.Entry;

/**
 *
 * @author besnik
 */
public class AccuracyComputation {

    public static void main(String[] args) {
        AccuracyComputation ac = new AccuracyComputation();

        ac.generateBaselineRanking("tfidf", "yovisto_tfidf.obj", "yovisto_tfidf_dpbedia_500.obj", "yovisto", 50, "");
    }

    /**
     * Generates the ranked set of entities and categories for the baseline
     * strategies.
     *
     * @param baseline_type
     * @param baseline_path
     * @param baseline_annotations
     * @param datasetname
     * @param topic_term_no
     * @param output_path
     */
    public void generateBaselineRanking(String baseline_type, String baseline_path, String baseline_annotations, String datasetname, int topic_term_no, String output_path) {
        if (baseline_type.equals("tfidf")) {
            //load the corresponding baseline terms and their annotations
            Map<String, Double> tfidfterms = (Map<String, Double>) FileUtils.readObject(baseline_path);
            Set<DBPediaAnnotation> tfidf_annotations = (Set<DBPediaAnnotation>) FileUtils.readObject(baseline_annotations);

            Map<Double, Set<String>> sortedtfidfterms = new TreeMap<Double, Set<String>>();
            for (String term : tfidfterms.keySet()) {
                Set<String> subsortedtfidfterms = sortedtfidfterms.get(tfidfterms.get(term));
                subsortedtfidfterms = subsortedtfidfterms == null ? new HashSet<String>() : subsortedtfidfterms;
                sortedtfidfterms.put(tfidfterms.get(term), subsortedtfidfterms);

                subsortedtfidfterms.add(term);
            }

            //for the varying term sizes construct the ranked topics
            Map<Integer, Set<String>> sub_terms = new TreeMap<Integer, Set<String>>();
            next_subset:
            for (int i = 50; i <= 200; i += 50) {
                Set<String> terms = new HashSet<String>();
                int counter = 0;

                for (Double tfidfvalue : sortedtfidfterms.keySet()) {
                    Set<String> subsortedtfidfterms = sortedtfidfterms.get(tfidfvalue);
                    for (String term : subsortedtfidfterms) {
                        if (counter >= i) {
                            sub_terms.put(i, terms);
                            continue next_subset;
                        }
                        terms.add(term);
                        counter++;
                    }
                }

                sub_terms.put(i, terms);
            }

            //for each of the different subsets of terms sorted based on Tfxidf generate the ranked topics
            Map<Integer, Map<String, List<Entry<DBPediaAnnotation, Double>>>> sub_annotations = new TreeMap<Integer, Map<String, List<Entry<DBPediaAnnotation, Double>>>>();
            for (int k : sub_terms.keySet()) {
                Set<String> k_terms = sub_terms.get(k);
                Map<String, List<Entry<DBPediaAnnotation, Double>>> k_sub_annotations = sub_annotations.get(k);
                k_sub_annotations = k_sub_annotations == null ? new TreeMap<String, List<Entry<DBPediaAnnotation, Double>>>() : k_sub_annotations;
                sub_annotations.put(k, k_sub_annotations);

                for (DBPediaAnnotation dbpa : tfidf_annotations) {
                    if (k_terms.contains(dbpa.surfacefrom)) {
                        List<Entry<DBPediaAnnotation, Double>> dbp_concept = k_sub_annotations.get(dbpa.uri);
                        dbp_concept = dbp_concept == null ? new ArrayList<Entry<DBPediaAnnotation, Double>>() : dbp_concept;
                        k_sub_annotations.put(dbpa.uri, dbp_concept);

                        dbp_concept.add(new AbstractMap.SimpleEntry<DBPediaAnnotation, Double>(dbpa, tfidfterms.get(dbpa.surfacefrom)));
                    }
                }
            }

            for (int k : sub_annotations.keySet()) {
                Map<String, List<Entry<DBPediaAnnotation, Double>>> k_sub_annotations = sub_annotations.get(k);
                StringBuilder sbentities = new StringBuilder();
                StringBuilder sbcategories = new StringBuilder();

                for (String dbpuri : k_sub_annotations.keySet()) {
                    double max_score = 0;
                    double sum_score = 0;
                    DBPediaAnnotation dbp_concept = null;
                    List<Entry<DBPediaAnnotation, Double>> dbp_concepts = k_sub_annotations.get(dbpuri);
                    for (Entry<DBPediaAnnotation, Double> entry : dbp_concepts) {
                        dbp_concept = entry.getKey();
                        double score = entry.getValue();

                        sum_score += score;
                        if (max_score < score) {
                            max_score = score;
                        }
                    }
                    //aggregated dbpedia entity score
                    sum_score /= dbp_concepts.size();
                    //add the categories entry.
                    addCategoriesString(dbp_concept.category, sbcategories, sum_score, -1);
                    //add the entity entry.
                    sbentities.append(dbpuri).append("\t").append(max_score).append("\n");
                }

                //write the generated content
                FileUtils.saveText(sbentities.toString(), output_path + datasetname + "_entities_tfidf_" + k + ".txt");
                FileUtils.saveText(sbcategories.toString(), output_path + datasetname + "_categories_tfidf_" + k + ".txt");
            }

        } else if (baseline_type.equals("lda")) {
            //load the corresponding baseline terms and their annotations
            Map<Integer, Set<Entry<String, Double>>> ldatopics = (Map<Integer, Set<Entry<String, Double>>>) FileUtils.readObject(baseline_path);
            Map<Integer, Set<DBPediaAnnotation>> lda_annotations = (Map<Integer, Set<DBPediaAnnotation>>) FileUtils.readObject(baseline_annotations);

            //for each topic assess the generated topics.
            StringBuilder sbentities = new StringBuilder();
            StringBuilder sbcategories = new StringBuilder();

            for (int topic : ldatopics.keySet()) {
                Set<Entry<String, Double>> topic_terms = ldatopics.get(topic);
                Set<DBPediaAnnotation> topic_annotations = lda_annotations.get(topic);

                if(topic_annotations == null || topic_annotations.isEmpty())
                    continue;
                
                Map<String, Double> ranked_terms = new TreeMap<String, Double>();
                for (Entry<String, Double> entry : topic_terms) {
                    ranked_terms.put(entry.getKey(), entry.getValue());
                }

                Map<String, List<Entry<DBPediaAnnotation, Double>>> dbp_concepts = new TreeMap<String, List<Entry<DBPediaAnnotation, Double>>>();
                for (DBPediaAnnotation dbp : topic_annotations) {
                    List<Entry<DBPediaAnnotation, Double>> sub_dbp_concepts = dbp_concepts.get(dbp.uri);
                    sub_dbp_concepts = sub_dbp_concepts == null ? new ArrayList<Entry<DBPediaAnnotation, Double>>() : sub_dbp_concepts;
                    dbp_concepts.put(dbp.uri, sub_dbp_concepts);

                    Double score = ranked_terms.get(dbp.surfacefrom);

                    if (score == null) {
                        for (String term : ranked_terms.keySet()) {
                            if (term.contains(dbp.surfacefrom)) {
                                score = ranked_terms.get(term);
                                break;
                            }
                        }
                    }
                    if (score == null) {
                        continue;
                    }
                    sub_dbp_concepts.add(new AbstractMap.SimpleEntry<DBPediaAnnotation, Double>(dbp, score));
                }

                for (String dbpuri : dbp_concepts.keySet()) {
                    double max_score = 0;
                    double sum_score = 0;
                    DBPediaAnnotation dbp_concept = null;
                    List<Entry<DBPediaAnnotation, Double>> dbp_concepts_detailed = dbp_concepts.get(dbpuri);
                    for (Entry<DBPediaAnnotation, Double> entry : dbp_concepts_detailed) {
                        dbp_concept = entry.getKey();
                        double score = entry.getValue();
                        sum_score += score;
                        if (max_score < score) {
                            max_score = score;
                        }
                    }
                    //aggregated dbpedia entity score
                    sum_score /= dbp_concepts.size();
                    //add the categories entry.
                    if (dbp_concept != null) {
                        addCategoriesString(dbp_concept.category, sbcategories, sum_score, topic);
                        //add the entity entry.
                        sbentities.append(topic).append("\t").append(dbpuri).append("\t").append(max_score).append("\n");
                    }
                }
            }
            //write the generated content
            FileUtils.saveText(sbentities.toString(), output_path + datasetname + "_entities_lda_" + ldatopics.size() + "_" + topic_term_no + ".txt");
            FileUtils.saveText(sbcategories.toString(), output_path + datasetname + "_categories_lda_" + ldatopics.size() + "_" + topic_term_no + ".txt");
        }
    }

    private void addCategoriesString(CategoryAnnotation cat, StringBuilder sb, double score, int topic) {
        if (cat == null || cat.categoryname == null || cat.categoryname.isEmpty()) {
            return;
        }
        sb.append(topic).append("\t").append(cat.categoryname).append("\t").append(score).append("\t").append(cat.level).append("\n");
        if (cat.children != null && !cat.children.isEmpty()) {
            for (CategoryAnnotation catchild : cat.children) {
                addCategoriesString(catchild, sb, score, topic);
            }
        }
    }
}
