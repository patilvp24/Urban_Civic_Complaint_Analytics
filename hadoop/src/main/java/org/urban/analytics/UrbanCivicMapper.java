package org.urban.analytics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * UrbanCivicMapper parses normalized NYC 311 TSV records and emits
 * multi-dimensional aggregation keys with MetricWritable values.
 */
public class UrbanCivicMapper extends Mapper<LongWritable, Text, Text, MetricWritable> {

    private final Text outKey = new Text();
    private final MetricWritable outVal = new MetricWritable(1L, 0.0);
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.isEmpty() || line.startsWith("unique_key\t")) {
            // Header line or empty record
            return;
        }

        String[] fields = line.split("\t", -1);
        if (fields.length < 8) {
            context.getCounter("UrbanAnalytics", "MALFORMED_RECORDS").increment(1);
            return;
        }

        String uniqueKey = fields[0].trim();
        String createdStr = fields[1].trim();
        String closedStr = fields[2].trim();
        String agency = fields[3].trim().isEmpty() ? "UNKNOWN" : fields[3].trim();
        String complaintType = fields[4].trim().isEmpty() ? "UNKNOWN" : fields[4].trim();
        // fields[5] is descriptor
        String status = fields[6].trim().isEmpty() ? "Unknown" : fields[6].trim();
        String borough = fields[7].trim().isEmpty() ? "UNSPECIFIED" : fields[7].trim();
        String lat = fields.length > 8 ? fields[8].trim() : "";
        String lon = fields.length > 9 ? fields[9].trim() : "";

        // Track total records
        context.getCounter("UrbanAnalytics", "TOTAL_INPUT_RECORDS").increment(1);
        emitCount(context, "QUALITY", "TOTAL_RECORDS");

        // 1. Complaint Category
        emitCount(context, "CATEGORY", complaintType);

        // 2. Agency Volume
        emitCount(context, "AGENCY", agency);

        // 3. Status Distribution
        emitCount(context, "STATUS", status);

        // 4. Monthly Trend (YYYY-MM)
        if (createdStr.length() >= 7) {
            String yearMonth = createdStr.substring(0, 7);
            emitCount(context, "MONTH", yearMonth);
        }

        // 5. Borough Distribution
        emitCount(context, "BOROUGH", borough);
        if ("UNSPECIFIED".equalsIgnoreCase(borough)) {
            emitCount(context, "QUALITY", "UNSPECIFIED_BOROUGH");
        }

        // 6. Cross Breakdowns (Agency x Status & Borough x Category)
        emitCount(context, "AGENCY_STATUS", agency + "::" + status);
        emitCount(context, "BOROUGH_CATEGORY", borough + "::" + complaintType);

        // 7. Coordinates Quality
        if (!lat.isEmpty() && !lon.isEmpty()) {
            emitCount(context, "QUALITY", "VALID_COORDINATES");
        } else {
            emitCount(context, "QUALITY", "MISSING_COORDINATES");
        }

        // 8. Resolution Time Analytics
        if (closedStr.isEmpty()) {
            // Open ticket
            emitCount(context, "QUALITY", "OPEN_REQUESTS");
            context.getCounter("UrbanAnalytics", "OPEN_REQUESTS").increment(1);
        } else {
            try {
                LocalDateTime createdDate = LocalDateTime.parse(createdStr, formatter);
                LocalDateTime closedDate = LocalDateTime.parse(closedStr, formatter);
                long diffMillis = Duration.between(createdDate, closedDate).toMillis();
                double durationHours = diffMillis / 3600000.0;

                if (durationHours >= 0.0) {
                    // Valid closed resolution duration
                    emitResolution(context, "RES_AGENCY", agency, durationHours);
                    emitResolution(context, "RES_CATEGORY", complaintType, durationHours);
                    emitResolution(context, "RES_OVERALL", "ALL", durationHours);
                    emitCount(context, "QUALITY", "CLOSED_WITH_VALID_DURATION");
                    context.getCounter("UrbanAnalytics", "RESOLVED_VALID_DURATION").increment(1);
                } else {
                    // Inverted timestamp anomaly (closed before created)
                    emitCount(context, "QUALITY", "INVERTED_TIMESTAMPS");
                    context.getCounter("UrbanAnalytics", "INVERTED_TIMESTAMPS").increment(1);
                }
            } catch (Exception e) {
                emitCount(context, "QUALITY", "UNPARSEABLE_TIMESTAMPS");
                context.getCounter("UrbanAnalytics", "UNPARSEABLE_TIMESTAMPS").increment(1);
            }
        }
    }

    private void emitCount(Context context, String prefix, String entity)
            throws IOException, InterruptedException {
        outKey.set(prefix + "\t" + entity);
        outVal.setCount(1L);
        outVal.setSumDurationHours(0.0);
        context.write(outKey, outVal);
    }

    private void emitResolution(Context context, String prefix, String entity, double hours)
            throws IOException, InterruptedException {
        outKey.set(prefix + "\t" + entity);
        outVal.setCount(1L);
        outVal.setSumDurationHours(hours);
        context.write(outKey, outVal);
    }
}
