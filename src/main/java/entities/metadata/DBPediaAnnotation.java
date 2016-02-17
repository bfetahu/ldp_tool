package entities.metadata;

import java.io.Serializable;

public class DBPediaAnnotation implements Serializable {

    private static final long serialVersionUID = 1L;
    public String uri;
    //stores from where the annotation was generated
    public String surfacefrom;
    //category associations
    public CategoryAnnotation category;

    public DBPediaAnnotation(String uri) {
        this.uri = uri;
        // TODO Auto-generated constructor stub
        category = new CategoryAnnotation();
    }

    public String getAnnotationURI() {
        return uri;
    }
}
