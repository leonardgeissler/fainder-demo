package de.tuberlin.dima.fainder;

import com.google.gson.JsonArray;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;

public class LuceneSearch {
    private static final Logger logger = LoggerFactory.getLogger(LuceneSearch.class);
    private final Directory indexDir;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final StandardAnalyzer analyzer;
    private final QueryParser parser;

    public LuceneSearch(Path indexPath) throws IOException {
        indexDir = FSDirectory.open(indexPath);
        reader = DirectoryReader.open(indexDir);
        searcher = new IndexSearcher(reader);
        analyzer = new StandardAnalyzer();
        parser = new QueryParser("all", analyzer);
        parser.setDefaultOperator(QueryParser.Operator.OR); // Make parser more lenient
    }

    /**
     * @param query     The query string
     * @param maxNumber The number of documents to return
     * @return An ArrayList of Documents that match the query
     */
    public JsonArray search(String query, int maxNumber, BitSet filter) throws ParseException {
        // Escape special characters
        String escaped = QueryParser.escape(query);
        // Add wildcard after the term, not before
        String wildcardQuery = escaped + "*";
        Query parsed_query = parser.parse(wildcardQuery);

        // If no filter is provided, just use the parsed query
        if (filter == null) {
            try {
                logger.debug("Executing query without filter: {}", parsed_query);
                ScoreDoc[] hits = searcher.search(parsed_query, maxNumber).scoreDocs;
                StoredFields storedFields = searcher.storedFields();
                JsonArray jsonArray = new JsonArray();
                for (ScoreDoc scoreDoc : hits) {
                    int hit = scoreDoc.doc;
                    Document hitDoc = storedFields.document(hit);
                    logger.debug("Hit {}: {} (Score: {})", hit, hitDoc.get("name"), scoreDoc.score);
                    jsonArray.add(hit);
                }
                return jsonArray;
            } catch (IOException e) {
                logger.error(Arrays.toString(e.getStackTrace()));
                return null;
            }
        }

        // create filter for allowed ids
        Query idFilter = createFilter(filter);

        // combine query and filter
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(parsed_query, BooleanClause.Occur.MUST);
        builder.add(idFilter, BooleanClause.Occur.FILTER);

        BooleanQuery combinedQuery = builder.build();

        try {
            logger.debug("Executing query {}", combinedQuery);

            ScoreDoc[] hits = searcher.search(combinedQuery, maxNumber).scoreDocs;
            StoredFields storedFields = searcher.storedFields();
            JsonArray jsonArray = new JsonArray();
            for (ScoreDoc scoreDoc : hits) {
                int hit = scoreDoc.doc;
                Document hitDoc = storedFields.document(hit);
                logger.debug("Hit {}: {} (Score: {})", hit, hitDoc.get("name"), scoreDoc.score);
                jsonArray.add(hit);
            }
            return jsonArray;
        } catch (IOException e) {
            logger.error(Arrays.toString(e.getStackTrace()));
            return null;
        }
    }

    private Query createFilter(BitSet filter) {
        // TODO: Improve efficiency off this
        if (filter == null) {
            return null;
        }
        // just or TermQueries for id in filter
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < filter.length(); i++) {
            if (filter.get(i)) {
                builder.add(new TermQuery(new Term("id", String.valueOf(i))), BooleanClause.Occur.SHOULD);
            }
        }
        return builder.build();
    }

}
