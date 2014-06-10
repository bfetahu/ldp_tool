package utils_lod;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextUtils {

    public static String removeSpecialSymbols(String str) {
        String rst = str;
        rst = rst.replaceAll("&#224;", "à");
        rst = rst.replaceAll("&#243;", "ó");
        rst = rst.replaceAll("&#246;", "ö");
        rst = rst.replaceAll("&#232;", "è");
        rst = rst.replaceAll("&#237;", "í");
        rst = rst.replaceAll("&#225;", "á");
        rst = rst.replaceAll("&#252;", "ü");
        rst = rst.replaceAll("&#233;", "é");
        rst = rst.replaceAll("&#263;", "ć");
        rst = rst.replaceAll("&#353;", "š");

        return rst;
    }
    /*
     * Parses content by taking into account specific content which matches a specific regular expression.
     */

    public static void parseString(String regex, String content) {
        String[] lines = content.split("\n");

        Pattern p = Pattern.compile(regex);

        StringBuffer result = new StringBuffer();
        StringBuffer proceedings = new StringBuffer();

        for (String line : lines) {
            String tmp = line.trim();
            if (tmp.startsWith("<li style")) {

                Matcher m = p.matcher(tmp);
                if (m.find()) {
                    String str = tmp.subSequence(m.start(), m.end()).toString() + "/preflayout=flat/";
                    str = "http://dl.acm.org/" + str;
                    str = str.replaceAll("&", "&amp;");

                    String title = tmp.substring(tmp.indexOf(">", m.end()) + 1, tmp.indexOf("</a>"));

                    proceedings.append(title.trim());
                    proceedings.append("\t");
                    proceedings.append(str.trim());
                    proceedings.append("\n");

                    result.append(str);
                    result.append("\n");
                }
            }
        }

        FileUtils.saveText(result.toString(), "SeedURLProceedings");
        FileUtils.saveText(proceedings.toString(), "Proceedings");
    }


    public static boolean isTermContainedInSet(Set<String> values, String value) {
        if(values == null || values.isEmpty())
            return true;
        
        for (String valuecmp : values) {
            if (value.toLowerCase().trim().contains(valuecmp.toLowerCase().trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Measures the Jaccard distance between two sets of terms.
     *
     * We add a factor which takes into account the ratio of the overlapping
     * terms from the sets from a particular resources compared to its total
     * number of items in the set. This enables us to rank higher those
     * annotations, whose Jaccard index is low, due to the limited number of
     * items in its set, thus, proposing a normalized Jaccard index ratio.
     *
     * @param setA
     * @param setB
     * @return
     */
    public static double computeJaccardDistance(Set<String> setA, Set<String> setB) {
        double rst = 0;
        if (setA == null || setB == null) {
            return 0;
        }

        Set<String> tmpD = new HashSet<String>(setA);
        Set<String> tmpN = new HashSet<String>(setA);

        tmpD.retainAll(setB);
        tmpN.addAll(setB);

        //the normalized score of the Jaccard index, which takes into account 
        //the number of items shared by a resource with respect to an extracted annotation

        rst = (tmpD.size() / (double) tmpN.size());
        return rst;
    }

    /**
     * Tokenizes the resource uri by removing the base URI.
     *
     * @param res_uri
     * @return
     */
    public static String tokeniseResourceURI(String res_uri) {
        String res_uri_rst = res_uri;

        //usually the last appearance of "/" or "#" can be used to identify the base URI.
        if (res_uri_rst.contains("#")) {
            int last_index = res_uri_rst.lastIndexOf("#") + 1;
            last_index = last_index < res_uri.length() ? last_index : 0;
            res_uri_rst = res_uri_rst.substring(last_index);
        }
        if (res_uri_rst.contains("/")) {
            int last_index = res_uri_rst.lastIndexOf("/") + 1;
            last_index = last_index < res_uri.length() ? last_index : 0;
            res_uri_rst = res_uri_rst.substring(last_index);
        }

        return res_uri_rst;
    }


    /**
     * Splits the resource's text into an array of terms.
     *
     * @param token_str
     * @return
     */
    public static String[] getValueTokens(String token_str) {
        if (token_str.contains("_")) {
            token_str = token_str.replaceAll("_", " ").trim().toLowerCase();
            token_str = token_str.replaceAll("\n", " ").trim();
            token_str = token_str.replaceAll("\r", " ").trim();
            token_str = token_str.replaceAll("%", " ").trim();
            token_str = token_str.replaceAll("'", " ").trim();
            token_str = token_str.replaceAll("\"", " ").trim();
            token_str = token_str.replaceAll("\\W", "");

            return token_str.split(" ");
        }

        return token_str.split(" ");
    }


    /**
     * Computes the Jaccard similarity between two given snippets of text, by first removing the stop words.
     *
     * @param value_a
     * @param value_b
     * @param stop_words
     * @return
     */
    public static double getJaccardSimilarity(String value_a, String value_b, Set<String> stop_words) {
        String[] terms_a = getValueTokens(value_a);
        String[] terms_b = getValueTokens(value_b);

        Set<String> set_a = new HashSet<String>();
        set_a.addAll(Arrays.asList(terms_a));
        set_a.removeAll(stop_words);

        Set<String> set_b = new HashSet<String>();
        set_b.addAll(Arrays.asList(terms_b));
        set_b.removeAll(stop_words);

        Set<String> intersection = new HashSet<String>(set_a);
        intersection.retainAll(set_b);
        return intersection.size() / ((double) (set_a.size() + set_b.size() - intersection.size()));
    }

    /**
     * For a given datastructure and the order of comparison, the method computes their jaccard similarity for their given text of items.
     *
     * @param data_profiles
     * @param comparison_order
     * @param stop_words
     * @return
     */
    public static Map<String, Map<String, Double>> getJaccardSimilarity(Map<String, String> data_profiles, Map<String, Set<String>> comparison_order, Set<String> stop_words) {
        Map<String, Map<String, Double>> jaccard_sim = new HashMap<String, Map<String, Double>>();

        for (String res_a : comparison_order.keySet()) {
            Map<String, Double> sub_jaccard = new HashMap<String, Double>();
            jaccard_sim.put(res_a, sub_jaccard);

            for (String res_b : comparison_order.get(res_a)) {
                sub_jaccard.put(res_b, getJaccardSimilarity(data_profiles.get(res_a), data_profiles.get(res_b), stop_words));
            }
        }

        return jaccard_sim;
    }
}
