/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class AolMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable k1, Text v1, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        String line = v1.toString().trim();
        if (line.isEmpty() || line.toLowerCase().startsWith("anonid")) {
            return;
        }
        String[] items = line.split("\t");
        if (items.length != 5) {
            return;
        }
        String query = items[1].trim();
        outputKey.set(query);
        outputValue.set(String.format("%s %s", items[3].trim(), items[4].trim()));
        oc.collect(outputKey, outputValue);
    }
}
