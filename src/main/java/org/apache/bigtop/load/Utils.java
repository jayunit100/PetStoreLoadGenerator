package org.apache.bigtop.load;

import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class Utils {

    public static HttpResponse get(String hostnam) throws Exception {
        URI uri = new URI(hostnam);
        return get(uri);
    }

    public static HttpResponse get(String hostname ,  String resource, List<? extends NameValuePair> params) throws Exception {
        System.out.println("getting http "+hostname);
        //URI uri = URIUtils.createURI("http", "www.google.com", -1, "/search",
          URI uri =
                  URIUtils.createURI("http", hostname, -1, resource,
                  URLEncodedUtils.format(params, "UTF-8"), null);
        System.out.println(uri.toASCIIString());
       HttpResponse respo =  get(uri);
       return respo;
    }

    public static HttpResponse get(URI uri) throws Exception {
        HttpGet httppost = new HttpGet(uri);
        HttpClient httpclient = HttpClients.createDefault();
        //System.out.println("sendindg " + httppost.getURI().toASCIIString());
        //Execute and get the response.
        try {
            HttpResponse response = httpclient.execute(httppost);
            if(response.getStatusLine().getStatusCode()!=200)
                System.err.println("FAILURE! " + response.getStatusLine().getStatusCode());
            return response;
        }
        catch (Throwable t) {
            System.out.println("failed, sleeing");
            Thread.sleep(10000);
        }
        System.err.println("FAILURE getting URI " + uri.toASCIIString());
        return null;
    }

    public static String json(Transaction t) throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(t) ;
    }

    public static String printable(Transaction t){
        return t.getStore().getId() + "," +
                t.getStore().getLocation().getZipcode() + "," +
                t.getStore().getLocation().getCity() + "," +
                t.getStore().getLocation().getState() + "," +
                t.getCustomer().getId() + "," +
                t.getCustomer().getName().getFirst()+ "," +
                t.getCustomer().getName().getSecond() + "," +
                t.getCustomer().getLocation().getZipcode() + "," +
                t.getCustomer().getLocation().getCity() + "," +
                t.getCustomer().getLocation().getState() + "," +
                t.getId() + "," +
                t.getDateTime() +","+
                join(t.getProducts(), ",");
    }

    //borrowed from apache.
    public static String join(Collection var0, Object var1) {
        StringBuffer var2 = new StringBuffer();

        for(Iterator var3 = var0.iterator(); var3.hasNext(); var2.append(var3.next())) {
            if(var2.length() != 0) {
                var2.append(var1);
            }
        }

        return var2.toString();
    }

}