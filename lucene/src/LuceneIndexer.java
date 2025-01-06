import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class LuceneIndexer {

    public static void createIndex(String lucenePath, String dataPath) {
        try {
            // Create a directory to store the index
            Path indexPath = Paths.get(lucenePath);

            // TODO: implement a way to check if the index directory is up to date
            // Delete the existing index directory (if it exists)
            if (Files.exists(indexPath)) {
                System.out.println("Deleting old existing index directory...");
                Files.walk(indexPath)
                        .sorted((p1, p2) -> -p1.compareTo(p2))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            }

            // Create the index directory
            Files.createDirectories(indexPath);

            // Create a directory to store the index
            Directory directory = FSDirectory.open(indexPath);

            // Create an analyzer to process the text
            StandardAnalyzer analyzer = new StandardAnalyzer();

            // Create an IndexWriterConfig
            IndexWriterConfig config = new IndexWriterConfig(analyzer);

            // Create an IndexWriter
            IndexWriter writer = new IndexWriter(directory, config);

            Gson gson = new Gson();
            int count = 0;

            // Get a list of all JSON files in the data directory (..\data) and its subdirectories recursively

            Path dataDir = Paths.get(dataPath); // Convert the data path to a Path object

            // check if the data directory exists

            if (!Files.exists(dataDir)) {
                System.out.println("Data directory does not exist!");
                return;
            }

            List<Path> jsonFiles = JsonFileFinder.getAllJsonFiles(dataPath);

            if (jsonFiles.isEmpty()) {
                System.out.println("No JSON files found in the data directory!");
                return;
            }


            List<Path> selectedFiles = jsonFiles;
//            System.out.println(selectedFiles);
            // Create index for the selected files
            for (Path filePath : selectedFiles) {
                count++;
                // Read the content of the file
                String jsonContent = new String(Files.readAllBytes(filePath));

                // Parse the JSON content into a JsonObject
                JsonObject jsonObject = gson.fromJson(jsonContent, JsonObject.class);

                // Create a new Lucene Document
                Document document = new Document();

                // Iterate through all key-value pairs in the JSON object
                for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                    String key = entry.getKey();
                    JsonElement value = entry.getValue();
                    // Convert the value to a string
                    String valueStr = value.isJsonNull() ? "" : value.toString();
                    if(valueStr.equals(("NaN")))valueStr = "";
                    if (key.equals("dateModified")) {
                        // Assuming dateModified is best represented as a StringField
                        document.add(new StringField(key, valueStr, Field.Store.YES));
                    }else if (key.equals("isAccessibleForFree")) {
                        // Assuming dateModified is best represented as a StringField
                        document.add(new StringField(key, valueStr, Field.Store.YES));
                    }else if (value.isJsonPrimitive()) {
                        JsonPrimitive primitive = value.getAsJsonPrimitive();
                        if (primitive.isString()) {
                            document.add(new TextField(key, primitive.getAsString(), Field.Store.YES));
                        } else {
                            document.add(new StringField(key, valueStr, Field.Store.YES));
                        }
                    } else {
                        document.add(new TextField(key, valueStr, Field.Store.YES));
                    }
                }
                // add the file path as a field named "path"
                document.add(new StringField("path", filePath.toString(), Field.Store.YES));

                // add the full file as a field named "all" to make it searchable
                document.add(new TextField("all", jsonContent, Field.Store.YES));

//                System.out.println(document);
                // Add the document to the index
                writer.addDocument(document);
            }
            System.out.println(count + "\n");
            writer.commit();
            writer.close();

            System.out.println("Index created successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        // get lucene path and dataset from args
        if (args.length != 2) {
            System.out.println("Usage: java LuceneIndexer <index_path>" + " <dataset_path>");
            return;
        }
        String lucenePath = args[0];
        String dataPath = args[1];

        createIndex(lucenePath, dataPath);

    }
}
