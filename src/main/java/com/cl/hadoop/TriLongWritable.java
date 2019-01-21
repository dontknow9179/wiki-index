package com.cl.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TriLongWritable {
    public long x;
    public long y;
    public long z;

    public TriLongWritable() {
        super();
    }
    public TriLongWritable(long x_, long y_, long z_) {
        x = x_;
        y = y_;
        z = z_;
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        y = in.readLong();
        z = in.readLong();
    }
    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeLong(y);
        out.writeLong(z);
    }

    @Override
    public String toString() {
        return String.valueOf(x) + "," + String.valueOf(y) + "," + String.valueOf(z);
    }
}

