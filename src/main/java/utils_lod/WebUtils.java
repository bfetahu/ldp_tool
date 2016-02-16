package utils_lod;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class WebUtils {

    private final static Logger LOGGER = Logger.getLogger(WebUtils.class.getName());
    public static HttpClient client = new HttpClient();

    /**
     * Does the actual call of the Web Service, using a HttpClient which executes the GetMethod.
     *
     * @param url webservice url
     * @return
     */
    public static String request(String url) {
        WebResource resource = Client.create().resource(url);
        ClientResponse response = resource.accept("application/json").get(ClientResponse.class);

        if (response.getStatus() == 200) {
            return response.getEntity(String.class);
        }

        return "";
    }

    /**
     * Make a post request (with parameters) to a given url
     *
     * @param url
     * @param urlParameters
     * @return
     */
    public static String post(String url, List<Map.Entry<String, String>> urlParameters) {
        try {
            PostMethod method = new PostMethod(url);

            // add header
            for (Map.Entry<String, String> name_val : urlParameters) {
                method.addParameter(name_val.getKey(), name_val.getValue());
            }

            //Set the results type, which will be JSON.
            method.addRequestHeader(new Header("Accept", "application/json"));
            method.addRequestHeader(new Header("content-type", "application/x-www-form-urlencoded"));

            if (client.executeMethod(method) != HttpStatus.SC_OK) {
                LOGGER.warning("Method failed: " + method.getStatusText());
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            String response_str = new String(responseBody);
            LOGGER.info(response_str);

            return response_str;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return "";
    }
}
