package org.apache.bigtop.load;

import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jayunit100 on 2/28/15.
 */
public class Utils {

    public static void get(URL path , String request) throws Exception {
        HttpClient httpclient = HttpClients.createDefault();
        System.out.println(path + " " + path.toURI() + " " +path.toURI().getPath());
        HttpPost httppost = new HttpPost(path.toURI());

        // Request parameters and other properties.
        List<NameValuePair> params = new java.util.ArrayList<NameValuePair>(2);
        //params.add(new BasicNameValuePair("param-1", "12345"));
        //params.add(new BasicNameValuePair("param-2", "Hello!"));
        //httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

        //Execute and get the response.
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            InputStream instream = entity.getContent();
            try {
                // do something useful
            } finally {
                instream.close();
            }
        }
        System.out.println("response : " + response.getStatusLine().getStatusCode());
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
