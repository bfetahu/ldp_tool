package pipeline;

import dataset_exporter.DatasetExporter;
import entities.linkeddata.Dataset;
import entities.metadata.DBPediaAnnotation;
import sparql_utils.annotation.ExtractAnnotations;
import sparql_utils.datadiscovery.EndpointUtils;
import sparql_utils.resource_sampling.OnlineResourceSampling;
import topic_ranking.DatasetAnnotationGraphUtils;
import utils_lod.FileUtils;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResourceApp {

	private final static Logger LOGGER = Logger.getLogger(ResourceApp.class.getName());

	public static void main(String[] args) {
        try {
	        Map<String, String> props = FileUtils.readIntoStringMap(args[0], "=", false);

	        //load the dataset metadata
	        if (props.get("loadcase").equals("0")) {
		        //the datastructure for managing and querying endpoints at ckan.
		        EndpointUtils ep = new EndpointUtils();

		        if (props.get("is_dataset_group_search").equals("true")) {
			        List<Dataset> ckandata = ep.loadGroupDatasetInformation(props.get("query_str"), props.get("outdir") + "/Logging.txt", true);
			        for (Dataset dataset : ckandata) {
				        LOGGER.info(dataset.name + " processed.");
				        FileUtils.saveObject(dataset, props.get("datasetpath") + "/" + dataset.name);
			        }
		        } else {
			        Dataset dataset = ep.packageSearchSingle(props.get("query_str"));
			        LOGGER.info(dataset.name + " processed.");
			        FileUtils.saveObject(dataset, props.get("datasetpath") + "/" + dataset.name);
		        }
	        }  //extract the resource instances from each dataset and resource type
	        else if (props.get("loadcase").equals("1")) {
		        Set<String> datasetpaths = new HashSet<String>();
		        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);

		        OnlineResourceSampling ors = new OnlineResourceSampling(props);
		        for (String dataset_path : datasetpaths) {
			        Dataset dataset = (Dataset) FileUtils.readObject(dataset_path);
			        ors.indexDataset(dataset);
		        }
	        }//perform NED on the textual literal of the datasets.
	        else if (props.get("loadcase").equals("2")) {
		        Map<String, DBPediaAnnotation> annotations = new HashMap<>();
		        Set<String> property_lookup = FileUtils.readIntoSet(props.get("property_lookup"), "\n", false);

		        Set<String> datasetpaths = new HashSet<String>();
		        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);

		        ExtractAnnotations ea = new ExtractAnnotations(props);
		        ea.performNED(datasetpaths, property_lookup, annotations);

		        LOGGER.info(annotations.size() + "");
		        //save the annotations
		        FileUtils.saveObject(annotations, props.get("annotationindex"));
	        } // perform topic filtering and ranking
	        else if (props.get("loadcase").equals("3")) {
		        Set<String> datasetpaths = new HashSet<String>();
		        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);
		        DatasetAnnotationGraphUtils dag = new DatasetAnnotationGraphUtils(props);
		        //compute the normalised score of topics and pre-filter noisy topics
		        dag.computeNormalisedScores();
		        //construct first the raw dataset topic graphs.
		        dag.constructDatasetTopicGraph();
		        //after constructing the dataset topic graph rank the topics
		        dag.rankTopics(datasetpaths);
	        }
	        //export to JSON the profiles output
	        else if (props.get("loadcase").equals("4")) {
		        Set<String> datasetpaths = new HashSet<String>();
		        FileUtils.getFilesList(props.get("datasetpath"), datasetpaths);

		        //sampling strategies for which we need to generate the corresponding graphs.
		        String[] ranking_strategies = props.get("topic_ranking_strategies").split(",");
		        Set<String> ranking_types = new HashSet<String>();
		        ranking_types.addAll(Arrays.asList(ranking_strategies));

		        DatasetExporter.getJSONRepresentationDatasets(datasetpaths, props.get("outdir"), ranking_types, props.get("sampling_type"), props.get("sample_size"),
				        props.get("topic_ranking_objects"), props.get("normalised_topic_score"), props.get("annotationindex"));
	        }
        } catch (Exception e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
