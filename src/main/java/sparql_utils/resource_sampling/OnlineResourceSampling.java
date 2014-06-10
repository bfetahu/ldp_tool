package sparql_utils.resource_sampling;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import entities.linkeddata.Dataset;
import entities.linkeddata.Resource;
import utils_lod.FileUtils;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by besnik on 09/06/2014.
 */
public class OnlineResourceSampling {

	private final static Logger LOGGER = Logger.getLogger(OnlineResourceSampling.class.getName());

    private Map<String, String> props;

    public OnlineResourceSampling(Map<String, String> props) {
        this.props = props;
    }

    /**
     * Returns the number of resoruces from a dataset.
     *
     * @param dataset
     * @return
     */
    private int getMaxResourceNumber(Dataset dataset) {
        int rst = -1;
        try {
            String querystr = "SELECT (COUNT(DISTINCT ?resource_uri) AS ?resource_count) WHERE {?resource_uri ?property ?value}";
            QueryEngineHTTP qt = new QueryEngineHTTP(dataset.url, querystr);
            ResultSet results = qt.execSelect();
            if (results.hasNext()) {
                QuerySolution qs = results.next();
                return qs.get("?resource_count").asLiteral().getInt();
            }

        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return rst;
    }

    /**
     * Returns the set of resource URIs and the corresponding datatype properties. This is used in the weighted sampling approach.
     *
     * @param dataset
     * @return
     */
    private Map<String, Integer> getResourceDataTypes(Dataset dataset, int max_resources) {
        Map<String, Integer> rst = new HashMap<>();
        try {
            while (rst.size() <= max_resources - 1) {
                String filter_offset = " OFFSET " + rst.size() + " LIMIT " + max_resources;
                String querystr = "SELECT ?resource_uri (COUNT(DISTINCT ?property) AS ?datatype_properties) WHERE {?resource_uri ?property ?value} GROUP BY ?resource_uri " + filter_offset;
                QueryEngineHTTP qt = new QueryEngineHTTP(dataset.url, querystr);
                ResultSet results = qt.execSelect();

                while (results.hasNext()) {
                    QuerySolution qs = results.next();
                    rst.put(qs.get("?resource_uri").toString(), qs.get("?datatype_properties").asLiteral().getInt());
                }
            }
        } catch (Exception e) {
	        LOGGER.severe(e.getMessage());
        }
        return rst;
    }

    /**
     * Returns the set of resource URIs and the corresponding types. This is used in the centrality sampling approach.
     *
     * @param dataset
     * @return
     */
    private Map<String, Integer> getResourceTypes(Dataset dataset, int max_resources) {
        Map<String, Integer> rst = new HashMap<>();
        try {
            while (rst.size() < max_resources - 1) {
                String filter_offset = " OFFSET " + rst.size() + " LIMIT " + max_resources;
                String querystr = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                        "SELECT ?resource_uri (COUNT(DISTINCT ?value) AS ?type_count) WHERE {?resource_uri rdf:type ?value} GROUP BY ?resource_uri " + filter_offset;
                QueryEngineHTTP qt = new QueryEngineHTTP(dataset.url, querystr);
                ResultSet results = qt.execSelect();
                while (results.hasNext()) {
                    QuerySolution qs = results.next();
                    rst.put(qs.get("?resource_uri").toString(), qs.get("?type_count").asLiteral().getInt());
                }
            }
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
        }
        return rst;
    }

    /**
     * Returns the set of randomly picked resource URI's from a dataset.
     *
     * @param max_resource_no
     * @param resources
     * @return
     */
    public Set<String> randomDatasetIndexing(int max_resource_no, String[] resources) {
        int sample_size = Integer.valueOf(props.get("sample_size"));
        int resource_no = (int) (((double) sample_size / 100.0) * max_resource_no);

        Random rand = new Random();
        Set<String> sampled_resources = new HashSet<String>();
        while (sampled_resources.size() < resource_no) {
            String res_uri = resources[rand.nextInt(resources.length - 1)];
            sampled_resources.add(res_uri);
        }
        return sampled_resources;
    }

    /**
     * Returns the set of resource URI's that are picked through a weighted sampling approach.
     *
     * @param max_resource_no
     * @param resources
     * @param resource_types
     * @return
     */
    public Set<String> centralityDatasetIndexing(int max_resource_no, String[] resources, Map<String, Integer> resource_types) {
        int sample_size = Integer.valueOf(props.get("sample_size"));
        int resource_no = (int) (((double) sample_size / 100.0) * max_resource_no);
        Random rand = new Random();
        Set<String> sampled_resources = new HashSet<String>();

        //the maximum number of datatype properties used to a describe a resource
        int max_datatype_properties = Collections.max(resource_types.values());

        while (sampled_resources.size() < resource_no) {
            String res_uri = resources[rand.nextInt(resources.length - 1)];
            double res_weight = resource_types.get(res_uri) / (double) max_datatype_properties;

            double rand_val = rand.nextDouble();
            if (res_weight >= 1 - rand_val) {
                sampled_resources.add(res_uri);
            }
        }
        return sampled_resources;
    }

    /**
     * Returns the set of resource URI's that are picked through a weighted sampling approach.
     *
     * @param max_resource_no
     * @param resources
     * @param resource_datatypes
     * @return
     */
    public Set<String> weightedDatasetIndexing(int max_resource_no, String[] resources, Map<String, Integer> resource_datatypes) {
        int sample_size = Integer.valueOf(props.get("sample_size"));
        int resource_no = (int) (((double) sample_size / 100.0) * max_resource_no);
        Random rand = new Random();
        Set<String> sampled_resources = new HashSet<String>();

        //the maximum number of datatype properties used to a describe a resource
        int max_datatype_properties = Collections.max(resource_datatypes.values());

        while (sampled_resources.size() < resource_no) {
            String res_uri = resources[rand.nextInt(resources.length - 1)];
            double res_weight = resource_datatypes.get(res_uri) / (double) max_datatype_properties;

            double rand_val = rand.nextDouble();
            if (res_weight >= 1 - rand_val) {
                sampled_resources.add(res_uri);
            }
        }
        return sampled_resources;
    }

    /**
     * Indexes a dataset based on one of the sampling strategies.
     *
     * @param dataset
     */
    public void indexDataset(Dataset dataset) {
        String sampling_type = props.get("sampling_type");
        int max_resource_no = getMaxResourceNumber(dataset);
	    LOGGER.info("Dataset " + dataset.name + " has " + max_resource_no + " resources.");

        Map<String, Integer> resource_datatypes = getResourceDataTypes(dataset, max_resource_no);
        String[] resources = new String[resource_datatypes.size()];
        resource_datatypes.keySet().toArray(resources);

        Set<String> sampled_resources = null;

        if (sampling_type.equals("random")) {
            sampled_resources = randomDatasetIndexing(max_resource_no, resources);
        } else if (sampling_type.equals("weighted")) {
            sampled_resources = weightedDatasetIndexing(max_resource_no, resources, resource_datatypes);
        } else if (sampling_type.equals("centrality")) {
            Map<String, Integer> resource_types = getResourceTypes(dataset, max_resource_no);
            sampled_resources = centralityDatasetIndexing(max_resource_no, resources, resource_types);
        }

	    LOGGER.info("Sampled resources: " + sampled_resources.size() + "\t" + sampled_resources.toString());
        //index all resources.
        sampled_resources.parallelStream().forEach(resource_uri -> addResourceToDataset(dataset, resource_uri));

        //store the updated dataset
        FileUtils.saveObject(dataset, props.get("datasetpath") + "/" + dataset.name);
	    LOGGER.info("Finished indexing dataset: " + dataset.name);
    }

    /**
     * Indexes a dataset's resource bassed on its URI.
     *
     * @param dataset
     * @param resource_uri
     */
    private void addResourceToDataset(Dataset dataset, String resource_uri) {
        String query_str = "SELECT ?property ?value WHERE { <" + resource_uri + "> ?property ?value}";
        QueryEngineHTTP qt = new QueryEngineHTTP(dataset.url, query_str);
        ResultSet results = qt.execSelect();

        Resource resource = dataset.resources.get(resource_uri);
        resource = resource == null ? new Resource() : resource;
        dataset.resources.put(resource_uri, resource);

        resource.id = resource_uri;

        while (results.hasNext()) {
            QuerySolution qs = results.next();

            if (qs.get("?property").toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                resource.types.add(qs.get("?value").toString());
                continue;
            }

            Map.Entry<String, String> resourcevalues = new AbstractMap.SimpleEntry<String, String>(qs.get("?property").toString(), qs.get("?value").toString());
            resource.resourcevalues.add(resourcevalues);
        }
        qt.close();

    }
}
