package org.urban.analytics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

/**
 * UrbanCivicReducer reduces all counts and duration metrics,
 * calculating averages for resolution analytics and emitting final summaries.
 */
public class UrbanCivicReducer extends Reducer<Text, MetricWritable, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<MetricWritable> values, Context context)
            throws IOException, InterruptedException {

        long totalCount = 0L;
        double totalDuration = 0.0;

        for (MetricWritable mw : values) {
            totalCount += mw.getCount();
            totalDuration += mw.getSumDurationHours();
        }

        String keyStr = key.toString();

        if (keyStr.startsWith("RES_")) {
            double avgHours = totalCount > 0 ? (totalDuration / totalCount) : 0.0;
            double avgDays = avgHours / 24.0;
            outVal.set(String.format(Locale.US, "%d\t%.4f\t%.4f\t%.2f",
                    totalCount, avgHours, avgDays, totalDuration));
        } else {
            outVal.set(String.valueOf(totalCount));
        }

        context.write(key, outVal);
    }
}
