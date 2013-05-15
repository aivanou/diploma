/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import java.io.IOException;
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
public class AolStatisticsCounterMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    public void map(LongWritable k1, Text value, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        String strValue = value.toString().trim();
        if (strValue.isEmpty() || strValue.equalsIgnoreCase("\n")) {
            return;
        }
        String[] items = parseInput(strValue);
        if (items.length != 4) {
            return;
        }
        String query = items[1].trim();
        String session = items[0].trim();
        String url = items[3].trim();
        outputKey.set("queries");
        outputValue.set(query);
        oc.collect(outputKey, outputValue);
        outputKey.set("sessions");
        outputValue.set(session);
        oc.collect(outputKey, outputValue);
        outputKey.set("urls");
        outputValue.set(url);
        oc.collect(outputKey, outputValue);
    }

    private String[] parseInput(String input) {
        String[] its = input.trim().split("\t");
        if (its.length != 2) {
            return new String[0];
        }
        String[] outValue = its[1].split(" +");
        return new String[]{its[0], outValue[0],
                    outValue[1].substring(1, outValue[1].length() - 1), outValue[2]};
    }
}
