package de.tuberlin.dima.fainder;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

import java.util.BitSet;
import java.util.Collection;
import java.util.Arrays;

public class CustomCollectorManager implements CollectorManager<CustomCollector, TopDocs> {
    private final BitSet filterBitSet;
    private final int numHits;

    public CustomCollectorManager(int numHits, BitSet filterBitSet) {
        this.filterBitSet = filterBitSet;
        this.numHits = numHits;
    }

    @Override
    public CustomCollector newCollector() {
        return new CustomCollector(filterBitSet, numHits);
    }

    @Override
    public TopDocs reduce(Collection<CustomCollector> collectors) {
        int totalHits = 0;
        ScoreDoc[] allScoreDocs = new ScoreDoc[numHits * collectors.size()];
        int pos = 0;
        
        // Gather results
        for (CustomCollector collector : collectors) {
            TopDocs topDocs = collector.getTopDocs();
            totalHits += topDocs.totalHits.value();
            System.arraycopy(topDocs.scoreDocs, 0, 
                           allScoreDocs, pos, 
                           topDocs.scoreDocs.length);
            pos += topDocs.scoreDocs.length;
        }
        
        // Sort only the filled portion
        Arrays.sort(allScoreDocs, 0, pos, 
                   (a, b) -> Float.compare(b.score, a.score));
        
        // Return top N results
        ScoreDoc[] finalDocs = Arrays.copyOf(allScoreDocs, 
                                           Math.min(numHits, pos));
        
        return new TopDocs(new TotalHits(totalHits, 
                          TotalHits.Relation.EQUAL_TO), finalDocs);
    }
}
