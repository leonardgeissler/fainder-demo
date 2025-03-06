package de.tuberlin.dima.fainder;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.dima.fainder.LuceneSearch.SearchResult;
import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Files;
import java.util.Iterator;

public class LuceneSearchPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(LuceneSearchPerformanceTest.class);
    private static Path indexPath;
    private static Path dataPath;
    private static LuceneSearch boolFilterSearcher;
    private static LuceneSearch customFilterSearcher;
    private static SearchResult allResults;
    private static SearchResult lastResult;
    private static int NUM_ALL_RESULTS;
    private static final float MIN_SCORE = 0.0f;
    private static final int MAX_RESULTS = 10000000;
    private static final int NUM_ITERATIONS = 10;
    private static final Path CSV_PATH = Paths.get("../logs/internal_lucene/lucene_performance_test.csv");
    private static FileWriter csvWriter;

    @BeforeAll
    static void setUp() throws IOException {
        // Create test index if it doesn't exist
        Dotenv config = Dotenv.configure().ignoreIfMissing().load();
        String dataDir = config.get("DATA_DIR");
        String collectionName = config.get("COLLECTION_NAME");
        String croissantDir = config.get("CROISSANT_DIR", "croissant");
        String luceneDir = config.get("LUCENE_DIR", "lucene");
        if (dataDir == null || collectionName == null) {
            throw new DotenvException("Missing required configuration: DATA_DIR, COLLECTION_NAME");
        }
        indexPath = Paths.get(dataDir, collectionName, luceneDir);
        dataPath = Paths.get(dataDir, collectionName, croissantDir);
        if (Files.exists(indexPath) && Files.isDirectory(indexPath)) {
            logger.info("Index directory {} already exists. Skipping index creation", indexPath);
        }  else {
            LuceneIndexer.createIndex(indexPath, dataPath);
        }
        
        boolFilterSearcher = new LuceneSearch(indexPath, true);
        customFilterSearcher = new LuceneSearch(indexPath, false);

        Set<Integer> docIds = new HashSet<>();

        allResults = boolFilterSearcher.search("*a*", docIds, MIN_SCORE, MAX_RESULTS, false);

        NUM_ALL_RESULTS = allResults.docIds.size();

        // Create directories if they don't exist
        Files.createDirectories(CSV_PATH.getParent());
        
        // Initialize CSV writer with headers
        csvWriter = new FileWriter(CSV_PATH.toFile());
        csvWriter.append("timestamp;keyword;execution_time;additional_filter_size;filter_size;num_results;mode\n");
    }

    private void logPerformanceCsv(String keyword, long executionTime, float additionalFilterSize, 
                                 int filterSize, int numResults, String mode) throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        csvWriter.append(String.format("%s;%s;%d;%.1f;%d;%d;%s\n", timestamp, keyword, executionTime, additionalFilterSize, filterSize, numResults, mode));
        csvWriter.flush();
    }

    @Test
    void compareFilterPerformance() throws IOException {
        String[] queries = {
            "*a*", "test", "germany", "lung", "data", "a", "the"
        };

        Float[] additionalFilterSizes = {
            0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f
        };

        for (String query : queries) {
            logger.info("Testing query: {}", query);

            // Test without filter
            Set<Integer> emptyFilter = new HashSet<>();
            long startTime = System.nanoTime();
            SearchResult resultWithoutFiltering = boolFilterSearcher.search(query, emptyFilter, MIN_SCORE, MAX_RESULTS, false);
            long executionTime = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds

            logPerformanceCsv(query, executionTime, 0f, 0, resultWithoutFiltering.docIds.size(), "no_filter");

            // Test with bool filter
            for (Float additionalFilterSize : additionalFilterSizes) {
                int filterSize = (int) (NUM_ALL_RESULTS * additionalFilterSize);
                
                // Create filter set with actual document IDs from allResults
                Set<Integer> filterDocIds = new HashSet<>(resultWithoutFiltering.docIds);
                // Add IDs from allResults up to filterSize
                int currentSize = filterDocIds.size();
                Iterator<Integer> allDocsIterator = allResults.docIds.iterator();
                while (currentSize < filterSize && allDocsIterator.hasNext()) {
                    filterDocIds.add(allDocsIterator.next());
                    currentSize++;
                }

                startTime = System.nanoTime();
                SearchResult results = boolFilterSearcher.search(query, filterDocIds, MIN_SCORE, MAX_RESULTS, false);
                executionTime = (System.nanoTime() - startTime) / 1_000_000;

                logPerformanceCsv(query, executionTime, additionalFilterSize, filterSize, results.docIds.size(), "bool_filter");
            }

            // Test with custom collector
            startTime = System.nanoTime();
            SearchResult resultWithCustomCollector = customFilterSearcher.search(query, emptyFilter, MIN_SCORE, MAX_RESULTS, true);
            executionTime = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds

            logPerformanceCsv(query, executionTime, 0f, 0, resultWithCustomCollector.docIds.size(), "custom_collector");

            // Test with custom collector and filter
            for (Float additionalFilterSize : additionalFilterSizes) {
                int filterSize = (int) (NUM_ALL_RESULTS * additionalFilterSize);
                
                // Create filter set with just the first filterSize documents
                Set<Integer> filterDocIds = new HashSet<>();
                for (int i = 0; i < filterSize; i++) {
                    filterDocIds.add(i);  // Add sequential document IDs
                }
                // Add the query results to ensure they're included
                filterDocIds.addAll(resultWithoutFiltering.docIds);

                startTime = System.nanoTime();
                SearchResult results = customFilterSearcher.search(query, filterDocIds, MIN_SCORE, MAX_RESULTS, false);
                executionTime = (System.nanoTime() - startTime) / 1_000_000;

                logPerformanceCsv(query, executionTime, additionalFilterSize, filterSize, results.docIds.size(), "custom_collector");
            }
        }

        csvWriter.close();
        boolFilterSearcher.close();
        customFilterSearcher.close();
    }

    private long runPerformanceTest(LuceneSearch searcher, String query, Set<Integer> filterList) throws IOException {
        long totalTime = 0;

        for (int i = 0; i < NUM_ITERATIONS; i++) {
            long startTime = System.nanoTime();
            lastResult = searcher.search(query, filterList, MIN_SCORE, MAX_RESULTS, false);
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime) / 1_000_000; // Convert to milliseconds
        }

        return totalTime;
    }
}
