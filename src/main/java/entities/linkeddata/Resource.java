package entities.linkeddata;


import utils_lod.TextUtils;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

public class Resource implements Serializable {

    private static final long serialVersionUID = 7732343100448135213L;
    public String id;
    public String title;
    public Set<String> types;
    public static String regexhttp = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    public static String regexhtml = "<[a-zA-Z\\/][^>]*>";
    //store the set of values assigned to this resource
    public List<Entry<String, String>> resourcevalues;
    //store the annotations for the particular resource
    public  Map<String, String> annotations;

    public Resource() {
        resourcevalues = new ArrayList<Entry<String, String>>();
        annotations = new HashMap<String, String>();
        types = new HashSet<String>();
    }

    public String toString(Set<String> propertylookup) {
        StringBuffer sb = new StringBuffer();

        //sb.append(id + "\t rdf:type\t" + type + "\n");
        for (Entry<String, String> values : resourcevalues) {
            String propertyuri = values.getKey().toLowerCase();

            for (String propertylookupval : propertylookup) {
                if (propertyuri.contains(propertylookupval)) {
                    sb.append(values.getValue() + ". ");
                    break;
                }
            }
        }
        return sb.toString();
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append(id + "\t rdf:type\t" + types.toString() + "\n");
        for (Entry<String, String> values : resourcevalues) {
            String propertyuri = values.getKey().toLowerCase();

            sb.append(propertyuri).append("\t").append(values.getValue()).append(". ");
            break;
        }
        return sb.toString();
    }

    public boolean containsString(String filter) {
        String resstr = toString();

        return resstr.contains(filter);
    }

    public boolean hasAnnotations() {
        return !annotations.isEmpty();
    }

    /*
     * Generates a composite description of an extracted resources, based on all assigned values for its properties.
     */

    public String getCompositeDescription(Set<String> propertylookup, Set<String> propanalysis) {
        StringBuilder compositedesc = new StringBuilder();
        for (Entry<String, String> entry : resourcevalues) {
            //if the property label doesn't match one of the property label lookups, then we skip that particular value
            if (!TextUtils.isTermContainedInSet(propertylookup, entry.getKey()) || !propanalysis.contains(entry.getKey())) {
                continue;
            }

            String value = entry.getValue();
            value = value.replaceAll(regexhttp, "");
            value = value.replaceAll(regexhtml, "");
            value = value.replaceAll("\\^", "").trim();

            //if the same value is added from similar property instances, then skip it
            if (compositedesc.toString().contains(value)) {
                continue;
            }

            compositedesc.append(value);
            compositedesc.append(" \n");

        }
        return compositedesc.toString();
    }
}
