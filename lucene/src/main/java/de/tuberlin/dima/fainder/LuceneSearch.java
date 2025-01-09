package de.tuberlin.dima.fainder;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.CharArraySet;
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
        // Configure analyzer to keep stop words
        analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
        parser = new QueryParser("all", analyzer);
        //parser.setAllowLeadingWildcard(true);  // Allow wildcards at start of term
    }

    /**
     * @param query     The query string
     * @param maxNumber The number of documents to return
     * @return An ArrayList of Documents that match the query
     */
    public JsonObject search(String query, int maxNumber, BitSet filter, Float minScore) throws ParseException {
        // Escape special characters
        String escaped = QueryParser.escape(query);

        // Create a boolean query combining exact and fuzzy matching
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();


        // If no filter is provided, just use the parsed query
        if (filter != null) {
            // create filter for allowed ids
            Query idFilter = createFilter(filter);
            queryBuilder.add(idFilter, BooleanClause.Occur.FILTER);
        }
        // Exact match gets highest boost
        queryBuilder.add(parser.parse(escaped), BooleanClause.Occur.SHOULD);
        // Fuzzy match for typos
        queryBuilder.add(parser.parse(escaped + "~"), BooleanClause.Occur.SHOULD);
        // Prefix match for partial words
        queryBuilder.add(parser.parse(escaped + "*"), BooleanClause.Occur.SHOULD);

        Query parsed_query = queryBuilder.build();

        try {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(parsed_query, BooleanClause.Occur.MUST);
            BooleanQuery combinedQuery = builder.build();
            if (filter != null){
                logger.debug("Executing query with filter: {}", combinedQuery + " " + filter);
            }
            else {
                logger.debug("Executing query without filter: {}", parsed_query);
            }
            ScoreDoc[] hits = searcher.search(combinedQuery, maxNumber).scoreDocs;
            StoredFields storedFields = searcher.storedFields();
            JsonArray idArray = new JsonArray();
            JsonArray scoreArray = new JsonArray();
            for (ScoreDoc scoreDoc : hits) {
                int hit = scoreDoc.doc;
                Document hitDoc = storedFields.document(hit);
                logger.debug("Hit {}: {} (Score: {})", hit, hitDoc.get("name"), scoreDoc.score);
                if (minScore == null || scoreDoc.score >= minScore) {
                    idArray.add(hit);
                    scoreArray.add(scoreDoc.score);
                }
            }
            JsonObject result = new JsonObject();
            result.add("ids", idArray);
            result.add("scores", scoreArray);
            return result;
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
        BooleanQuery.Builder filterQuery = new BooleanQuery.Builder();
        filterQuery.add(builder.build(), BooleanClause.Occur.MUST);
        return builder.build();
    }

}
