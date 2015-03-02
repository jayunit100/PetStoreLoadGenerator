package org.apache.bigtop.load;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import junit.framework.Assert;
import org.apache.bigtop.load.LoadGen;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Ignore;
import sun.java2d.pipe.SpanShapeRenderer;
import sun.misc.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Locale;

/**
 * Created by jayunit100 on 2/25/15.
 */
public class TestEverything {

    @org.junit.Test
    public void testFileLoadGen(){
        new File("/tmp/transactions0.txt").delete();
        LoadGen.TESTING=true;
        LoadGen.main(new String[]{"/tmp","1","5","1500000","13241234"});
        Assert.assertTrue(new File("/tmp/transactions0.txt").length()>0);
    }

    /**
     * Only for testing realtime web app.
     */
    @org.junit.Test
    public void testWebLoadGen(){
        LoadGen.TESTING=true;
        LoadGen.main(new String[]{"http://localhost:3000/rpush/guestbook", "1", "5", "1500000", "13241234"});
    }

    /**
     * This is a generic test that our server wrappers etc are okay.
     * The code paths it exersizes aren't necessarily used via the
     * prime purpose of the app as of the march 1 2015.
     * @throws Exception
     */
    @org.junit.Test
    public void testURLGets() throws Exception {

        final String bind="localhost";
        final int port=8129;
        final String ctx="/";
        final String rsp = "This is the response...";

        SimpleHttpServer server = new SimpleHttpServer(port);

        List<NameValuePair> params = new java.util.ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("b", "2"));
        params.add(new BasicNameValuePair("b2", "2"));
        params.add(new BasicNameValuePair("b3", "2"));
        params.add(new BasicNameValuePair("bsdf", "2"));

        HttpResponse r = Utils.get("localhost:8129","", params);
        System.out.println(r );
        String contents = org.apache.commons.io.IOUtils.toString(r.getEntity().getContent());
        System.out.println(contents);
        Assert.assertTrue(contents.contains("bsdf === 2"));
        Assert.assertTrue(contents.contains("b === 2"));
        Assert.assertTrue(contents.contains("b3 === 2"));
        Assert.assertTrue(contents.contains("b2 === 2"));

        server.stop();

    }
}
