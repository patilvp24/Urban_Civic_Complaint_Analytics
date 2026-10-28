package org.urban.analytics;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * MetricWritable holds an aggregate count and duration sum.
 * Used by Mapper, Combiner, and Reducer to support associative
 * in-memory and shuffle-phase aggregation.
 */
public class MetricWritable implements Writable {
    private long count;
    private double sumDurationHours;

    public MetricWritable() {
        this.count = 0;
        this.sumDurationHours = 0.0;
    }

    public MetricWritable(long count, double sumDurationHours) {
        this.count = count;
        this.sumDurationHours = sumDurationHours;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSumDurationHours() {
        return sumDurationHours;
    }

    public void setSumDurationHours(double sumDurationHours) {
        this.sumDurationHours = sumDurationHours;
    }

    public void add(MetricWritable other) {
        this.count += other.count;
        this.sumDurationHours += other.sumDurationHours;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(sumDurationHours);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readLong();
        this.sumDurationHours = in.readDouble();
    }

    @Override
    public String toString() {
        return count + "\t" + sumDurationHours;
    }
}
