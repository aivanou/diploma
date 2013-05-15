/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

import java.io.IOException;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 * @author alex
 */
public class UserSessionRecordReader implements RecordReader<AvroWrapper<Pair<Text, UserSessionWritable>>, NullWritable> {

    public boolean next(AvroWrapper<Pair<Text, UserSessionWritable>> k, NullWritable v) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public AvroWrapper<Pair<Text, UserSessionWritable>> createKey() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public NullWritable createValue() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public long getPos() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public float getProgress() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
