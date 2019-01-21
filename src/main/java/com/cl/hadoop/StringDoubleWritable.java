package com.cl.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringDoubleWritable implements WritableComparable<StringDoubleWritable> {
    public String x;
    public double y;

    public StringDoubleWritable() {
        super();
    }
    public StringDoubleWritable(String x_, double y_) {
        x = x_;
        y = y_;
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readUTF();
        y = in.readDouble();
    }
    public void write(DataOutput out) throws IOException {
        out.writeUTF(x);
        out.writeDouble(y);
    }

    public int compareTo(StringDoubleWritable op) {
        int cmp = x.compareTo(op.x);
        if (cmp != 0) return cmp;
        return -(Double.valueOf(y).compareTo(op.y));
    }

    @Override
    public String toString() {
        return x + " " + String.format("%.2f", y) ;
    }

}

