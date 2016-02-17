package entities.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CategoryAnnotation implements Serializable {

    private static final long serialVersionUID = 6928606637779569401L;
    public String categoryname;
    public int level = 0;
    public List<CategoryAnnotation> children;

    public CategoryAnnotation() {
        children = new ArrayList<CategoryAnnotation>();
    }

    public boolean containsChild(String catname) {
        for (CategoryAnnotation child : children) {
            if (child.categoryname.equals(catname)) {
                return true;
            }
        }
        return false;
    }
}
