import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.*;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.Headers;

// Driver Class
public class LuceneServer {
    static LuceneSearch luceneSearch;
    static String pathIndex = "lucene_index";
    static String pathData = "data/kaggle/metadata";
    static Integer port = 8001;

    public LuceneServer() throws IOException {
        LuceneIndexer.createIndex(pathIndex, pathData);
        System.out.println("Creating Index");
        LuceneIndexer.main(new String[]{pathIndex});
        luceneSearch = new LuceneSearch(pathIndex);
    }

    // Main Method
    public static void main(String[] args) throws IOException {
        new LuceneServer();
        // Create an HttpServer instance
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        // Create a context for a specific path and set the handler
        server.createContext("/search", new MyHandler());

        // Start the server
        server.setExecutor(null); // Use the default executor
        server.start();

        System.out.println("Server is running on port " + port);
    }


    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Got Request");
            Headers headers = exchange.getResponseHeaders();
            headers.add("Access-Control-Allow-Origin", "*");
            headers.add("Access-Control-Allow-Methods", "POST, OPTIONS");
            headers.add("Access-Control-Allow-Headers", "Content-Type");

            String requestMethod = exchange.getRequestMethod();

            if (requestMethod.equalsIgnoreCase("OPTIONS")) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if (requestMethod.equalsIgnoreCase("POST")) {
                // Set response headers
                headers.add("Content-Type", "application/json");

                // Read the request body
                InputStreamReader isr = new InputStreamReader(exchange.getRequestBody());
                BufferedReader br = new BufferedReader(isr);
                StringBuilder requestBody = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    requestBody.append(line);
                }

                // Parse JSON body
                JsonObject jsonRequest = JsonParser.parseString(requestBody.toString()).getAsJsonObject();
                String keyword_query = jsonRequest.get("keywords").getAsString();

                System.out.println("Received query: " + keyword_query);

                JsonArray results;
                try {
                    results = luceneSearch.search(keyword_query, 10);
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                JsonObject responseObject = new JsonObject();
                responseObject.add("results", results);
                String response = responseObject.toString();

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                // Method not allowed
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}
