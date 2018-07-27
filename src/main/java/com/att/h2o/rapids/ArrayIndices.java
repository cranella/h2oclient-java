package com.att.h2o.rapids;

import java.util.Arrays;

public class ArrayIndices {

    private String value;
    public final static ArrayIndices ALL = new ArrayIndices("ALL");

    public ArrayIndices(long index) {
        this.value = Long.toString(index);
    }

    public ArrayIndices(long [] indices) {
        this.value = Arrays.toString(indices);
    }

    public ArrayIndices(long start, long end) {
        this.value = String.format("[%d:%d]",start,end-start);
    }

    private ArrayIndices(String value) {
        this.value = value;
    }

    public boolean isAll() {
        return value.equals("ALL");
    }

    @Override
    public String toString() {
        return value;
    }
}
