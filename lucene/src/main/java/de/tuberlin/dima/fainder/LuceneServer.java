package de.tuberlin.dima.fainder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class LuceneServer {
    private static final Logger logger = LoggerFactory.getLogger(LuceneServer.class);
    static LuceneSearch luceneSearch;
    static Path indexPath;
    static Path dataPath;
    static int port;
    static int maxResults;

    public LuceneServer() throws IOException {
        logger.info("Starting Lucene Server");

        // Load properties
        try {
            Properties properties = loadProperties("config.properties");
            logger.debug(String.valueOf(properties));

            String fainderHome = System.getenv("FAINDER_DEMO_HOME");
            if (fainderHome == null) {
                logger.error("FAINDER_DEMO_HOME environment variable not set");
                System.exit(1);
            }

            indexPath = Paths.get(fainderHome, properties.getProperty("indexPath"));
            dataPath = Paths.get(fainderHome, properties.getProperty("dataPath"));
            maxResults = Integer.parseInt(properties.getProperty("maxResults"));
            port = Integer.parseInt(properties.getProperty("port"));
        } catch (IOException e) {
            logger.error("Failed to load properties: {}", e.getMessage());
            System.exit(1);
        }

        if (Files.exists(indexPath) && Files.isDirectory(indexPath)) {
            logger.info("Index directory already exists. Skipping index creation");
        } else {
            logger.info("Creating index");
            try {
                LuceneIndexer.createIndex(indexPath, dataPath);
            } catch (RuntimeException e) {
                logger.error("Index creation failed: {}", e.getMessage());
                System.exit(1);
            }
        }
        luceneSearch = new LuceneSearch(indexPath);

        logger.info("Startup complete");
    }

    public static void main(String[] args) throws IOException {
        new LuceneServer();
        // Create an HttpServer instance
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        // Create a context for a specific path and set the handler
        server.createContext("/search", new SearchHandler());

        // Start the server
        server.setExecutor(null); // Use the default executor
        server.start();

        logger.info("Server is running on port {}", port);
    }

    public static Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = LuceneServer.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream == null) {
                throw new IOException("Property file '" + fileName + "' not found in the classpath");
            }
            properties.load(inputStream);
        }
        return properties;
    }

    static class SearchHandler implements HttpHandler {
        public void sendJson(HttpExchange exchange, int code, String payload) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, payload.length());
            OutputStream os = exchange.getResponseBody();
            os.write(payload.getBytes());
            os.close();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Headers headers = exchange.getResponseHeaders();
            headers.add("Access-Control-Allow-Origin", "*");
            headers.add("Access-Control-Allow-Methods", "POST, OPTIONS");
            headers.add("Access-Control-Allow-Headers", "Content-Type");

            switch (exchange.getRequestMethod().toUpperCase()) {
                case "OPTIONS":
                    logger.debug("Received OPTIONS request");
                    exchange.sendResponseHeaders(204, -1);
                    break;
                case "POST":
                    logger.debug("Received POST request");

                    // Check content type
                    if (!"application/json".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                        sendJson(exchange, 400, "{\"error\": \"Content-Type must be application/json\"}");
                        return;
                    }

                    // Read and parse the request body
                    String keywordQuery;
                    try (InputStreamReader isr = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)) {
                        JsonObject json = JsonParser.parseReader(isr).getAsJsonObject();
                        keywordQuery = json.get("keywords").getAsString();
                    } catch (JsonParseException e) {
                        sendJson(exchange, 400, "{\"error\": \"Invalid JSON format\"}");
                        return;
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                        return;
                    }

                    if (keywordQuery == null || keywordQuery.isEmpty()) {
                        sendJson(exchange, 400, "{\"error\": \"keywords field is missing or empty\"}");
                        return;
                    }

                    try {
                        JsonArray results = luceneSearch.search(keywordQuery, maxResults);
                        JsonObject responseObject = new JsonObject();
                        responseObject.add("results", results);
                        sendJson(exchange, 200, responseObject.toString());
                    } catch (ParseException e) {
                        sendJson(exchange, 400, "{\"error\":\"Query parsing error: " + e.getMessage() + "\"}");
                        logger.warn("Parsing error for query: {}", keywordQuery);
                    }

                    break;
                default:
                    logger.warn("Invalid request method: {}", exchange.getRequestMethod());
                    exchange.sendResponseHeaders(405, -1);
                    break;
            }
        }
    }
}
