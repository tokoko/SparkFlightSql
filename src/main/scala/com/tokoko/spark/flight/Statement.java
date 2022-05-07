package com.tokoko.spark.flight;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Statement {

    private VectorSchemaRoot root;

    private ConcurrentLinkedQueue<ArrowRecordBatch> batches;

    public Statement() {
        this.root = null;
        this.batches = new ConcurrentLinkedQueue<>();
    }

    public void setRoot(VectorSchemaRoot root) {
        this.root = root;
    }

    public VectorSchemaRoot getRoot() {
        return root;
    }

    public void addBatch(ArrowRecordBatch batch) {
        batches.add(batch);
    }

    public ArrowRecordBatch nextBatch() {
        return batches.poll();
    }

}
