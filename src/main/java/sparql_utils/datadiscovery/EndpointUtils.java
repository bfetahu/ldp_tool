package sparql_utils.datadiscovery;


import com.hp.hpl.jena.sparql.lib.org.json.JSONArray;
import com.hp.hpl.jena.sparql.lib.org.json.JSONObject;
import entities.linkeddata.Dataset;
import utils_lod.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class EndpointUtils {

	private final static Logger LOGGER = Logger.getLogger(EndpointUtils.class.getName());

    /*
     * Search string for the datasets within the group of linkededucation.
     */

    public final static String DATAHUBURL = "http://datahub.io/api/action/group_show";
    public final static String DATAHUBSINGLEURL = "http://datahub.io/api/action/package_show";
    public final static String DATAHUBPACKAGESEARCH = "http://datahub.io/api/action/package_search";
    public final static String DATAHUBRESOURCESEARCH = "http://datahub.io/api/action/resource_search";

    /*
     * Returns the list of datasets within a specific search query from
     * datahub.io.
     */
    public List<Dataset> loadGroupDatasetInformation(String groupid, String logpath, boolean isLogging) {
        try {
            List<Dataset> lst = new ArrayList<Dataset>();

            HttpPost post = new HttpPost(DATAHUBURL);
            post.setHeader("X-CKAN-API-Key", "bf317334-3107-4a25-9773-b5961ef3500b");
            StringEntity input = new StringEntity("{\"id\":\"" + groupid + "\"}");
            input.setContentType("application/json");
            post.setEntity(input);

            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(post);
            String responsestr = getResponseText(response);

            JSONObject jsobj = new JSONObject(responsestr).getJSONObject("result");
            JSONArray jsarr = jsobj.getJSONArray("packages");

	        LOGGER.info(jsarr.toString());

            for (int i = 0; i < jsarr.length(); i++) {
                JSONObject jsondataset = jsarr.getJSONObject(i);
	            LOGGER.info(jsondataset.toString());

                Dataset ds = new Dataset();
                ds.id = jsondataset.getString("id");
                ds.name = jsondataset.getString("name");
                ds.notes = jsondataset.getString("notes");
               // ds.url = jsondataset.getString("url");
                ds.maintainer = jsondataset.getString("maintainer");
                ds.maintainer_email = jsondataset.getString("maintainer_email");
                ds.author = jsondataset.getString("author");
                ds.author_email = jsondataset.getString("author_email");
                ds.capacity = jsondataset.getString("capacity");
                ds.licence_id = jsondataset.getString("license_id");
                ds.revision_id = jsondataset.getString("revision_id");
                ds.state = jsondataset.getString("state");
                ds.title = jsondataset.getString("title");
                ds.type = jsondataset.getString("type");
                //ds.version = jsondataset.getString("version");

                loadDatasetInformation(ds, ds.name);
                lst.add(ds);

	            LOGGER.info(i + "\t" + ds.name + " loaded");
            }
            return lst;
        } catch (Exception e) {
            if (isLogging) {
                LOGGER.severe("Method[loadGroupDatasetInformation]\nError occurred: " + e.getMessage());
                FileUtils.saveText(e.getLocalizedMessage(), logpath, true);
            }
        }
        return null;
    }

    /*
     * Returns the list of datasets within a specific search query from
     * datahub.io.
     */
    public Dataset packageSearchSingle(String datasetid) {
        try {
            Dataset ds = new Dataset();

            HttpPost post = new HttpPost(DATAHUBSINGLEURL);
            post.setHeader("X-CKAN-API-Key", "bf317334-3107-4a25-9773-b5961ef3500b");
            StringEntity input = new StringEntity("{\"id\":\"" + datasetid + "\"}");
            input.setContentType("application/json");
            post.setEntity(input);

            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(post);
            String responsestr = getResponseText(response);

	        LOGGER.info(responsestr);

            JSONObject jsondataset = new JSONObject(responsestr).getJSONObject("result");

            ds.id = jsondataset.getString("id");
            ds.name = jsondataset.getString("name");
            ds.title = jsondataset.getString("title");

            loadDatasetInformation(ds, ds.name);

            return ds;
        } catch (Exception e) {
	        LOGGER.severe("Method[packageSearch]\nError occurred: " + e.getMessage());
            FileUtils.saveText(e.getLocalizedMessage(), "ResourceUtilsLogging.txt", true);
        }
        return null;
    }
    /*
     * Returns the list of datasets within a specific search query from
     * datahub.io.
     */

    public List<Dataset> packageSearch(String query, String logpath, boolean isLogging) {
        try {
            List<Dataset> lst = new ArrayList<Dataset>();

            HttpPost post = new HttpPost(DATAHUBPACKAGESEARCH);
            post.setHeader("X-CKAN-API-Key", "bf317334-3107-4a25-9773-b5961ef3500b");
            StringEntity input = new StringEntity(query);
            input.setContentType("application/json");
            post.setEntity(input);

            HttpClient httpclient = new DefaultHttpClient();
            HttpResponse response = httpclient.execute(post);
            String responsestr = getResponseText(response);

	        LOGGER.info(responsestr);

            JSONObject jsobj = new JSONObject(responsestr).getJSONObject("result");
            JSONArray jsarr = jsobj.getJSONArray("results");

            for (int i = 0; i < jsarr.length(); i++) {
                JSONObject jsondataset = jsarr.getJSONObject(i);
                Dataset ds = new Dataset();
                ds.id = jsondataset.getString("id");
                ds.name = jsondataset.getString("name");
                ds.notes = jsondataset.getString("notes");
                ds.url = jsondataset.getString("url");
                ds.maintainer = jsondataset.getString("maintainer");
                ds.maintainer_email = jsondataset.getString("maintainer_email");
                ds.author = jsondataset.getString("author");
                ds.author_email = jsondataset.getString("author_email");
                ds.licence_id = jsondataset.getString("license_id");
                ds.revision_id = jsondataset.getString("revision_id");
                ds.state = jsondataset.getString("state");
                ds.title = jsondataset.getString("title");
                ds.type = jsondataset.getString("type");
                ds.version = jsondataset.getString("version");

                loadDatasetInformation(ds, ds.name);
                lst.add(ds);
            }
            return lst;
        } catch (Exception e) {
            if (isLogging) {
	            LOGGER.severe("Method[packageSearch]\nError occurred: " + e.getMessage());
                FileUtils.saveText(e.getLocalizedMessage(), logpath, true);
            }
        }
        return null;
    }

    /*
     * Returns the list of datasets within a specific search query from
     * datahub.io.
     */
    public void loadDatasetInformation(Dataset ds, String datasetid) throws Exception {
        HttpPost post = new HttpPost(DATAHUBSINGLEURL);
        post.setHeader("X-CKAN-API-Key", "bf317334-3107-4a25-9773-b5961ef3500b");
        StringEntity input = new StringEntity("{\"id\":\"" + datasetid + "\"}");
        input.setContentType("application/json");
        post.setEntity(input);

        HttpClient httpclient = new DefaultHttpClient();
        HttpResponse response = httpclient.execute(post);
        String responsestr = getResponseText(response);

        JSONObject jsondataset = new JSONObject(responsestr).getJSONObject("result");
        JSONArray jsondatasetres = jsondataset.getJSONArray("resources");

        //load the SPARQL endpoint
        for (int i = 0; i < jsondatasetres.length(); i++) {
            JSONObject jsontmp = jsondatasetres.getJSONObject(i);
            String strname = "", strdesc = "", format = "";
            if (!jsontmp.isNull("name")) {
                strname = jsontmp.getString("name");
            }
            if (!jsontmp.isNull("description")) {
                strdesc = jsontmp.getString("description");
            }
            if (!jsontmp.isNull("format")) {
                format = jsontmp.getString("format");
            }

            if (strname.contains("SPARQL") || strdesc.contains("SPARQL") || format.contains("sparql")) {
                ds.url = jsontmp.getString("url");
            }
        }
    }

    private static String getResponseText(HttpResponse response) {
        String body = "";
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
            String line = "";
            while ((line = br.readLine()) != null) {
                body += line;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return body;
    }

    /**
     * Loads the set of datasets previously loaded at a certain path.
     *
     * @param path
     * @return
     */
    public List<Dataset> loadDataset(String path) {
        List<Dataset> lst = new ArrayList<Dataset>();

        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(path, files);

        for (String file : files) {
            lst.add((Dataset) FileUtils.readObject(file));
        }
        return lst;
    }
}
