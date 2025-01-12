package de.tuberlin.dima.fainder;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.List;

public class CustomCollector implements Collector {
    private final List<Integer> filterDocIds;
    private final TopScoreDocCollector topScoreDocCollector;

    public CustomCollector(List<Integer> filterDocIds, int numHits) throws IOException {
        this.filterDocIds = filterDocIds;
        TopScoreDocCollectorManager topScoreDocCollectorManager = new TopScoreDocCollectorManager(numHits, Integer.MAX_VALUE);
        this.topScoreDocCollector = topScoreDocCollectorManager.newCollector();
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        LeafCollector leafCollector = topScoreDocCollector.getLeafCollector(context);

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                leafCollector.setScorer(scorer);
            }

            @Override
            public void collect(int doc) throws IOException {
                if (filterDocIds.contains(doc)) {
                    leafCollector.collect(doc);
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    public TopDocs getTopDocs() {
        return topScoreDocCollector.topDocs();
    }
}
