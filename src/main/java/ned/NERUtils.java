/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ned;


import com.hp.hpl.jena.sparql.lib.org.json.JSONArray;
import com.hp.hpl.jena.sparql.lib.org.json.JSONObject;
import utils_lod.WebUtils;

import java.util.*;
import java.util.logging.Logger;

/**
 * @author besnik
 */
public class NERUtils {

	private final static Logger LOGGER = Logger.getLogger(NERUtils.class.getClass().getName());

    private Map<String, String> props;

    public NERUtils(Map<String, String> props) {
        this.props = props;
    }

    /**
     * Performs the NED process of textual literals using the DBpedia
     * Spotlight NER tool. The named entities are in the form of DBpedia entity
     * URIs.
     *
     * @param doc
     * @return
     */
    public Map<String, Map.Entry<String, Double>> performDBpediaSpotlightNER(String doc) {
        String API_URL = "http://spotlight.dbpedia.org/rest/annotate/";
        double CONFIDENCE = 0.3;
        int SUPPORT = 20;
        String powered_by = "non";
        String spotter = "CoOccurrenceBasedSelector";
        String disambiguator = "Default";//Default ;Occurrences=Occurrence-centric;Document=Document-centric
        String showScores = "yes";
        //store the annotation in the map data structure.
        Map<String, Map.Entry<String, Double>> map = new HashMap<String, Map.Entry<String, Double>>();

        try {
            List<Map.Entry<String, String>> urlParameters = new ArrayList<Map.Entry<String, String>>();
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("spotter", spotter));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("disambiguator", disambiguator));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("showScores", showScores));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("text", doc));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("support", SUPPORT + ""));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("confidence", CONFIDENCE + ""));

            String response = WebUtils.post(API_URL, urlParameters);
            //parse the response in JSON format
            JSONObject resultJSON = new JSONObject(response);

            if (resultJSON.has("Resources")) {
                JSONArray entities = resultJSON.getJSONArray("Resources");

                for (int i = 0; i < entities.length(); i++) {
                    JSONObject entity = entities.getJSONObject(i);

                    if (!entity.has("@URI")) {
                        continue;
                    }

                    //store the annotation information
                    AbstractMap.SimpleEntry<String, Double> entry = new AbstractMap.SimpleEntry<String, Double>(entity.getString("@URI"), entity.getDouble("@similarityScore"));
                    map.put(entity.getString("@surfaceForm"), entry);

					LOGGER.info(entity.getString("@surfaceForm") + "\t" + entity.getString("@URI"));
                }
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }

        return map;
    }

    /**
     * Performs the NED process of textual literals using the DBpedia
     * Spotlight NER tool. The named entities are in the form of DBpedia entity
     * URIs.
     *
     * @param doc
     * @return
     */
    public Map<String, String> performSimpleDBpediaSpotlightNER(String doc) {
        String API_URL = "http://spotlight.dbpedia.org/rest/annotate/";
        double CONFIDENCE = 0.3;
        int SUPPORT = 20;
        String powered_by = "non";
        String spotter = "CoOccurrenceBasedSelector";
        String disambiguator = "Default";//Default ;Occurrences=Occurrence-centric;Document=Document-centric
        String showScores = "yes";
        //store the annotation in the map data structure.
        Map<String, String> map = new HashMap<String, String>();

        try {
            List<Map.Entry<String, String>> urlParameters = new ArrayList<Map.Entry<String, String>>();
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("spotter", spotter));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("disambiguator", disambiguator));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("showScores", showScores));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("text", doc));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("support", SUPPORT + ""));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("confidence", CONFIDENCE + ""));

            String response = WebUtils.post(API_URL, urlParameters);
            //parse the response in JSON format
            JSONObject resultJSON = new JSONObject(response);

            if (resultJSON.has("Resources")) {
                JSONArray entities = resultJSON.getJSONArray("Resources");

                for (int i = 0; i < entities.length(); i++) {
                    JSONObject entity = entities.getJSONObject(i);

                    if (!entity.has("@URI") || entity.getDouble("@similarityScore") < 0.1) {
                        continue;
                    }

                    //store the annotation information
                    map.put(entity.getString("@surfaceForm"), entity.getString("@URI"));
                }
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }

        return map;
    }

    /**
     * Performs the NER process on textual literals using the TagMe NER
     * tool. The named entities are in the form of Wikipedia entity URIs.
     *
     * @param doc
     * @return
     */
    public Map<String, Object> performTagMeNER(String doc) {
        String API_URL = "http://tagme.di.unipi.it/tag";
        String API_KEY = props.get("tagme_api_key");
        String epsilon = "0.1";

        //store the annotation in the map data structure.
        Map<String, Map.Entry<String, Double>> map = new HashMap<String, Map.Entry<String, Double>>();
        //store the start and end of the keyword appearance for which the entities are extracted from.
        Map<String, Map<String, Map.Entry<Integer, Integer>>> entity_start_end = new HashMap<String, Map<String, Map.Entry<Integer, Integer>>>();

        Map<String, Object> ner_results = new HashMap<String, Object>();
        ner_results.put("ner_results", map);
        ner_results.put("ner_start_offset", entity_start_end);

        try {
            List<Map.Entry<String, String>> urlParameters = new ArrayList<Map.Entry<String, String>>();
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("key", API_KEY));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("epsilon", epsilon));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("text", doc));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("include_abstract", "false"));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("include_categories", "false"));

            String response = WebUtils.post(API_URL, urlParameters);

            //parse the json output from TagMe.
            JSONObject resultJSON = new JSONObject(response);
            if (resultJSON.has("annotations")) {
                JSONArray annotations = resultJSON.getJSONArray("annotations");

                //store the annotations
                for (int i = 0; i < annotations.length(); i++) {
                    JSONObject annotation = annotations.getJSONObject(i);

                    if (!annotation.has("title")) {
                        continue;
                    }

                    String title_wiki_page = annotation.getString("title");
                    int start = annotation.getInt("start");
                    double rho = annotation.getDouble("rho");
                    int end = annotation.getInt("end");
                    String spot = annotation.getString("spot");

                    String entity_url = "http://en.wikipedia.org/wiki/" + title_wiki_page.replaceAll(" ", "_");
                    AbstractMap.SimpleEntry<String, Double> entry = new AbstractMap.SimpleEntry<String, Double>(entity_url, rho);
                    map.put(spot, entry);

                    //add for each spot the entity and its corresponding start and offset appearance in the article.
                    Map<String, Map.Entry<Integer, Integer>> sub_entity_start_end = entity_start_end.get(spot);
                    sub_entity_start_end = sub_entity_start_end == null ? new HashMap<String, Map.Entry<Integer, Integer>>() : sub_entity_start_end;
                    entity_start_end.put(spot, sub_entity_start_end);

                    sub_entity_start_end.put(entity_url, new AbstractMap.SimpleEntry<Integer, Integer>(start, end));

                }
            }

        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }

        return ner_results;
    }

    /**
     * Performs the NER process on textual literals using the TagMe NER
     * tool. The named entities are in the form of Wikipedia entity URIs.
     *
     * @param doc
     * @return
     */
    public Map<String, String> performSimpleTagMeNER(String doc) {
        String API_URL = "http://tagme.di.unipi.it/tag";
        String API_KEY = props.get("tagme_api_key");
        String epsilon = "0.1";

        //store the annotation in the map data structure.
        Map<String, String> map = new HashMap<String, String>();

        try {
            List<Map.Entry<String, String>> urlParameters = new ArrayList<Map.Entry<String, String>>();
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("key", API_KEY));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("epsilon", epsilon));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("text", doc));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("include_abstract", "false"));
            urlParameters.add(new AbstractMap.SimpleEntry<String, String>("include_categories", "false"));

            String response = WebUtils.post(API_URL, urlParameters);

            //parse the json output from TagMe.
            JSONObject resultJSON = new JSONObject(response);
            if (resultJSON.has("annotations")) {
                JSONArray annotations = resultJSON.getJSONArray("annotations");

                //store the annotations
                for (int i = 0; i < annotations.length(); i++) {
                    JSONObject annotation = annotations.getJSONObject(i);

                    if (!annotation.has("title")) {
                        continue;
                    }

                    String title_wiki_page = annotation.getString("title");
                    int start = annotation.getInt("start");
                    double rho = annotation.getDouble("rho");
                    int end = annotation.getInt("end");
                    String spot = annotation.getString("spot");

                    if (rho < 0.1) {
                        continue;
                    }

                    String entity_url = "http://dbpedia.org/resource/" + title_wiki_page.replaceAll(" ", "_");
                    map.put(spot, entity_url);
                }
            }

        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }

        return map;
    }
}
