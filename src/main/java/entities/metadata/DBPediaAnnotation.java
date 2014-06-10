package entities.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class DBPediaAnnotation implements Serializable {

    private static final long serialVersionUID = 1L;
    public String uri;
    public int support;
    //stores from where the annotation was generated
    public String surfacefrom;
    //using standard IR similarity measures, we compute how likely is the annotation to be correct given the context of the resource.	
    public double annotationconfidence;
    //each annotation has also an additional description and label extracted from DBpedia
    public Entry<String, String> extraannotation;
    //category associations
    public CategoryAnnotation category;

    public DBPediaAnnotation(String uri) {
        this.uri = uri;
        // TODO Auto-generated constructor stub
        category = new CategoryAnnotation();
    }

    public DBPediaAnnotation(String uri, int support) {
        this.uri = uri;
        this.support = support;
        category = new CategoryAnnotation();
    }

    public String getCategoryRepresentation() {
        Set<String> catset = new HashSet<String>();

        for (CategoryAnnotation cat : category.children) {
            category.getStringRepresentation(cat, catset);
        }

        return catset.toString();
    }

    public String getAnnotationURI() {
        return uri;
    }

    public Set<String> getSubCategoryRepresentation(Set<String> sb, CategoryAnnotation category) {
        sb.add(category.categoryname);

        if (category.children != null && !category.children.isEmpty()) {
            for (CategoryAnnotation child : category.children) {
                getSubCategoryRepresentation(sb, child);
            }
        }
        return sb;
    }
}
