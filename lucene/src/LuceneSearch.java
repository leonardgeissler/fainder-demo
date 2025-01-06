import com.google.gson.JsonArray;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneSearch {
    private IndexSearcher searcher;
    private IndexReader reader;
    private Directory directory;
    private StandardAnalyzer analyzer;
    private QueryParser parser;

    public LuceneSearch(String pathIndex) throws IOException {
        directory = FSDirectory.open(Paths.get(pathIndex));
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        analyzer = new StandardAnalyzer();
        parser = new QueryParser("all", analyzer);
        parser.setDefaultOperator(QueryParser.Operator.OR); // Make parser more lenient
    }

    /**
     * @param query  The query string
     * @return An ArrayList of Documents that match the query
     */
    public JsonArray search(String query, int maxNumber) throws ParseException {
        // Escape special characters
        String escaped = QueryParser.escape(query);
        // Add wildcard after the term, not before
        String wildcardQuery = escaped + "*";
        Query parsed_query = parser.parse(wildcardQuery);

        try {
            System.out.println("Search Results");

            ScoreDoc[] hits = searcher.search(parsed_query, maxNumber).scoreDocs;
            StoredFields storedFields = searcher.storedFields();
            JsonArray jsonArray = new JsonArray();
            for (ScoreDoc scoreDoc : hits) {
                int hit = scoreDoc.doc;
                Document hitDoc = storedFields.document(hit);
                System.out.println("Hit nr:" + hit);
                System.out.println("Hit: " + hitDoc.get("name") + " Score: " + scoreDoc.score);
                jsonArray.add(hit);
            }
            return jsonArray;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
