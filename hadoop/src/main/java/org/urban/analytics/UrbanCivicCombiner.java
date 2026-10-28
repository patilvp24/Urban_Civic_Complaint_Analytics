package org.urban.analytics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * UrbanCivicCombiner pre-aggregates MetricWritable values on the Map side
 * before intermediate shuffle, minimizing network and disk I/O.
 */
public class UrbanCivicCombiner extends Reducer<Text, MetricWritable, Text, MetricWritable> {

    private final MetricWritable outVal = new MetricWritable();

    @Override
    protected void reduce(Text key, Iterable<MetricWritable> values, Context context)
            throws IOException, InterruptedException {

        long totalCount = 0L;
        double totalDuration = 0.0;

        for (MetricWritable mw : values) {
            totalCount += mw.getCount();
            totalDuration += mw.getSumDurationHours();
        }

        outVal.setCount(totalCount);
        outVal.setSumDurationHours(totalDuration);
        context.write(key, outVal);
    }
}
