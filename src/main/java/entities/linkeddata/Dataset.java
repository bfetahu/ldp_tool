package entities.linkeddata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Dataset implements Serializable {

    private static final long serialVersionUID = 5077448742783249961L;
    public String id;
    public String maintainer_email;
    public String revision_id;
    public String state;
    public String type;
    public String maintainer;
    public String url;
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
}
