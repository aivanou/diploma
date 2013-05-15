/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models.objects;

import java.io.IOException;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class UserSessionInputFormat
        extends SequenceFileInputFormat<Text, UserSessionWritable> {

    @Override
    public RecordReader<AvroWrapper<Pair<Text, UserSessionWritable>>, NullWritable> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter)
            throws IOException {
        
        return new UserSessionRecordReader();
    }
}
