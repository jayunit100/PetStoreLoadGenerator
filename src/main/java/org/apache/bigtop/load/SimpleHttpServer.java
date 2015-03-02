package org.apache.bigtop.load;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * This is just a server impl for unit testing.
 * The purpose of having the code in this repository is for debugging and simplicity
 * of the implementation, but we could at some point replace it with a better mock library.
 */
public class SimpleHttpServer {

    public static void main(String[] args){
        try{
            SimpleHttpServer s=new SimpleHttpServer(8123);
            Thread.sleep(100000);
            s.stop();
        }
        catch(Exception e){
            e.printStackTrace();;
        }
    }

    HttpServer server = null;
    public SimpleHttpServer(int port) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost",port), 1);
        server.createContext("/", new GetHandler());
        server.createContext("/info", new InfoHandler());
        //server.createContext("/get", new GetHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    public void stop(){
        server.stop(1);
    }


    // http://localhost:8000/info
    static class InfoHandler implements HttpHandler {
        public void handle(HttpExchange httpExchange) throws IOException {
            String response = "Use /get?hello=word to see parameters.";
            SimpleHttpServer.writeResponse(httpExchange, response.toString());
        }
    }

    static class GetHandler implements HttpHandler {
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder response = new StringBuilder();

            Map <String,String> parms = SimpleHttpServer.queryToMap(httpExchange.getRequestURI().toASCIIString());
            response.append("<html><body>");
            for(String k:parms.keySet())
                response.append(" " + k + " === " + parms.get(k));
            response.append("</body></html>");
            SimpleHttpServer.writeResponse(httpExchange, response.toString());
        }
    }

    public static void writeResponse(HttpExchange httpExchange, String response) throws IOException {
        try {
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
        catch(Throwable t){
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }


    /**
     * returns the url parameters in a map
     * @param query
     * @return map
     */
    public static Map<String, String> queryToMap(String q){
        //after the slash.
        String query=q.substring(2);
        Map<String, String> result = new HashMap<String, String>();
        if(query==null){
            System.out.println("QUERY IS NULL") ;
            return result;
        }
        System.out.println(query);
        for (String param : query.split("&")) {
            System.out.println("reading param on server : " + param + " " + query);
            String pair[] = param.split("=");
            if (pair.length>1) {
                result.put(pair[0], pair[1]);
            }else{
                result.put(pair[0], "");
            }
        }
        return result;
    }

}

