package org.apache.bigtop.load;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import junit.framework.Assert;
import org.apache.bigtop.load.LoadGen;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
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

    @org.junit.Test
    public void testURLGets() throws Exception {
        /**
         * Create a server...
         */
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost",8189), 0);
        try {
            class MyHandler implements HttpHandler {
                public void handle(HttpExchange t) throws IOException {
                    System.out.println("HANDLE !!!!!!!");
                    String response = "This is the response";
                    t.sendResponseHeaders(200, response.length());
                    OutputStream os = t.getResponseBody();
                    os.write(response.getBytes());
                    os.close();
                }
            }
            server.createContext("/", new MyHandler());
            server.createContext("/test", new MyHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
            Utils.get(new URL("http://localhost:8189"), "blah");
        }
        catch(Throwable t){

        }
        finally{
            System.out.println("CLosing");
        }

        /**
         * Kill the server
         */
        server.stop(1);
    }
}
