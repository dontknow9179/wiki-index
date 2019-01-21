package com.cl.hadoop;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class LongLongWritable implements Writable {
    public long x;
    public long y;

    public LongLongWritable() {
        super();
    }
    public LongLongWritable(long x_, long y_) {
        x = x_;
        y = y_;
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        y = in.readLong();
    }
    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeLong(y);
    }

    @Override
    public String toString() {
        return "(" + String.valueOf(x) + "," + String.valueOf(y) + ")";
    }
}

