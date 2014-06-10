package sparql_utils.repositories;

import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

public class PropertiesUtil {
	private String enrichmentOptionsProperties;
	private String enrichmentOptionsClasses;
	private ResourceBundle configuration;
	private String sesameServer;
	private String repositoryID;
	private static PropertiesUtil instance = null;

	protected PropertiesUtil() {
		configuration = ResourceBundle.getBundle("de.unihannover.l3s.arcomem.conf");
		enrichmentOptionsProperties = configuration.getString("enrichment.options.properties");
		enrichmentOptionsClasses = configuration.getString("enrichment.options.classes");
		sesameServer = configuration.getString("sesame.store");
		repositoryID = configuration.getString("sesame.repositoryID");
	}

	public String getSesameServer() {
		return sesameServer;
	}

	public String getRepositoryID() {
		return repositoryID;
	}

	public String getEnrichmentOptionsProperties() {
		return enrichmentOptionsProperties;
	}

	public List<String> getEnrichmentOptionsPropertiesList() {
		return Arrays.asList(enrichmentOptionsProperties.split(";"));
	}

	public String getEnrichmentOptionsClasses() {
		return enrichmentOptionsClasses;
	}

	public List<String> getEnrichmentOptionsClassesList() {
		return Arrays.asList(enrichmentOptionsClasses.split(";"));
	}
	
	public static PropertiesUtil getInstance() {
		if (instance == null) {
			instance = new PropertiesUtil();
		}
		return instance;
	}

}
