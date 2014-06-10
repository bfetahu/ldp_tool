package entities.linkeddata;

import java.io.Serializable;
import java.util.*;

public class Dataset implements Serializable {

    private static final long serialVersionUID = 5077448742783249961L;
    public String id;
    public String maintainer_email;
    public String revision_id;
    public String state;
    public String type;
    public String maintainer;
    public String url;
    public String version;
    public String author;
    public String title;
    public String author_email;
    public String name;
    public String capacity;
    public String licence_id;
    public String notes;
    public Set<String> tags;
    public Set<String> groups;
    public Set<String> types;
    public Map<String, Resource> resources;

    public Dataset() {
        tags = new HashSet<String>();
        groups = new HashSet<String>();
        types = new HashSet<String>();
        resources = new TreeMap<String, Resource>();
    }

    /**
     * Returns the set of all entities with which are annotated the dataset's resources.
     *
     * @return
     */
    public Map<String, Set<String>> getDatasetAnnotationEntities() {
        Map<String, Set<String>> annotations = new HashMap<>();
        for (String res_uri : resources.keySet()) {
            Resource resource = resources.get(res_uri);

            for (String entity : resource.annotations.values()) {
                Set<String> sub_annotations = annotations.get(entity);
                sub_annotations = sub_annotations == null ? new HashSet<>() : sub_annotations;
                annotations.put(entity, sub_annotations);

                sub_annotations.add(res_uri);
            }
        }
        return annotations;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();

        sb.append("ID:\t" + id + "\n");
        sb.append("Maintainer:\t" + maintainer + "\n");
        sb.append("Maintainer E-Mail:\t" + maintainer_email + "\n");
        sb.append("Title:\t" + title + "\n");
        sb.append("Notes:\t" + notes + "\n");
        sb.append("URL:\t" + url + "\n");
        return sb.toString();
    }

    public boolean hasValidResourceClasses() {
        return this.types != null && this.types.size() != 0;
    }

    /**
     * JSON string representation of a dataset.
     *
     * @return
     */
    public String getJSONDatasetRepresentation() {
        StringBuffer sb = new StringBuffer();

        sb.append("\t\"Dataset\":{\n");
        sb.append("\t\"URI\":\"" + url + "\",\n");
        sb.append("\t\"Name\":\"" + name + "\",\n");
        sb.append("\t\"Description\":\"" + title + "\",\n");

        return sb.toString();
    }
}
