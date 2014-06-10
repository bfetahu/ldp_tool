package sparql_utils.annotation;


import entities.linkeddata.Dataset;
import entities.linkeddata.Resource;
import entities.metadata.DBPediaAnnotation;
import ned.NERUtils;
import org.apache.commons.lang.StringUtils;
import utils_lod.FileUtils;

import java.util.*;
import java.util.logging.Logger;

public class ExtractAnnotations {

	private final static Logger LOGGER = Logger.getLogger(ExtractAnnotations.class.getName());

    private Map<String, String> props;
    private NERUtils ned;

    public ExtractAnnotations(Map<String, String> props) {
        this.props = props;
        ned = new NERUtils(props);
    }

    public void performNED(Set<String> datasetpaths, Set<String> property_lookup, Map<String, DBPediaAnnotation> annotations) {
        datasetpaths.parallelStream().forEach(dataset_path -> annotateDatasets(dataset_path, property_lookup, annotations));
    }

    private void annotateDatasets(String dataset_path, Set<String> property_lookup, Map<String, DBPediaAnnotation> annotations) {
        Dataset dataset = (Dataset) FileUtils.readObject(dataset_path);
        if (dataset == null || dataset.resources == null || dataset.resources.isEmpty()) {
            return;
        }

        Set<String> textprops = mapDatasetProperties(dataset);

        for (String resourcekey : dataset.resources.keySet()) {
            Resource resource = dataset.resources.get(resourcekey);
            if (resourcekey.contains("dbpedia")) {
                continue;
            }
            String resourcecontent = resource.getCompositeDescription(property_lookup, textprops);
            if(resourcecontent.trim().isEmpty()){
                continue;
            }

            Map<String, String> annotations_keys = null;
            if(props.get("ned_operation").equals("tagme")){
                annotations_keys = ned.performSimpleTagMeNER(resourcecontent);
            }
            else{
                annotations_keys = ned.performSimpleDBpediaSpotlightNER(resourcecontent);
            }

            resource.annotations = annotations_keys;

            ExtractCategoryAnnotations eca = new ExtractCategoryAnnotations();
            //append all the ontology objects
            for (String surfacefrom : annotations_keys.keySet()) {
                if (!annotations.containsKey(annotations_keys.get(surfacefrom))) {
                    DBPediaAnnotation dbpobj = annotations.get(annotations_keys.get(surfacefrom));
                    dbpobj = dbpobj == null ? new DBPediaAnnotation(annotations_keys.get(surfacefrom)) : dbpobj;
                    dbpobj.uri = annotations_keys.get(surfacefrom);
                    dbpobj.surfacefrom = surfacefrom;
                    dbpobj.category = eca.getCategories(dbpobj.uri, Long.valueOf(props.get("timeout")), props.get("dbpedia_url"));

                    annotations.put(dbpobj.getAnnotationURI(), dbpobj);
                }
            }
	        LOGGER.info("Annotating resource: " + resource.id + " finisehd");
        }

        FileUtils.saveObject(dataset, dataset_path);
    }

    /**
     * Maps the different datatype properties based on their similarity so that
     * they not considered twice for each operation that we run in our dataset
     * analysis.
     *
     * @param dataset
     * @return
     */
    public static Set<String> mapDatasetProperties(Dataset dataset) {
        Map<String, Double> propertyanalysis = analyseDatasetProperties(dataset);

        Set<String> props = new HashSet<String>();
        //add the remaining properties.
        for (String property : propertyanalysis.keySet()) {
            if (propertyanalysis.get(property) > 0.8) {
                props.add(property);
            }
        }
        return props;
    }

    /**
     * Analyses which of the properties assigned to the different resource types
     * and instances are string values. It ignores numerical, date and URI
     * values.
     *
     * @param dataset
     * @return
     */
    public static Map<String, Double> analyseDatasetProperties(Dataset dataset) {
        Map<String, Map<String, List<Boolean>>> propertyanalysis = new TreeMap<String, Map<String, List<Boolean>>>();
        for (String resuri : dataset.resources.keySet()) {
            Resource res = dataset.resources.get(resuri);

            for (String type : res.types) {
                Map<String, List<Boolean>> subpropertyanalysis = propertyanalysis.get(type);
                subpropertyanalysis = subpropertyanalysis == null ? new TreeMap<String, List<Boolean>>() : subpropertyanalysis;
                propertyanalysis.put(type, subpropertyanalysis);

                for (Map.Entry<String, String> instance : res.resourcevalues) {
                    List<Boolean> subsubpropertyanalysis = subpropertyanalysis.get(instance.getKey());
                    subsubpropertyanalysis = subsubpropertyanalysis == null ? new ArrayList<Boolean>() : subsubpropertyanalysis;
                    subpropertyanalysis.put(instance.getKey(), subsubpropertyanalysis);

                    boolean isLiteral = StringUtils.isNumeric(instance.getValue()) ||
                            instance.getValue().matches("http(s?)://.*?(/?)(?=\\s|$|(\\p{Punct}(\\s|$)))") ||
                            instance.getValue().matches("[0-9]*?-[0-9]*?-[0-9]{2}.*") || instance.getValue().matches("[0-9]+?\\.[0-9]*");
                    ;
                    subsubpropertyanalysis.add(!isLiteral);
                }
            }
        }

        Map<String, Double> rst = new TreeMap<String, Double>();
        Map<String, Integer> rstcnt = new TreeMap<String, Integer>();

        for (String type : propertyanalysis.keySet()) {
            Map<String, List<Boolean>> subpropertyanalysis = propertyanalysis.get(type);

            for (String property : subpropertyanalysis.keySet()) {
                List<Boolean> subsubpropertyanalysis = subpropertyanalysis.get(property);
                int count = 0;
                for (Boolean tmp : subsubpropertyanalysis) {
                    if (tmp) {
                        count++;
                    }
                }

                Double val = rst.get(property);
                Integer cnt = rstcnt.get(property);

                cnt = cnt == null ? 1 : cnt + 1;

                if (val == null) {
                    val = 0.0;
                }

                val += (double) count / subsubpropertyanalysis.size();

                rst.put(property, val);
                rstcnt.put(property, cnt);
            }
        }

        for (String property : rst.keySet()) {
            rst.put(property, rst.get(property) / rstcnt.get(property));
        }
        return rst;
    }
}
