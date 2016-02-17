package utils_lod;

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
