/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class AolStatisticsCounterReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    private final Set<Integer> valueHashes = new HashSet<Integer>();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
        String strKey = key.toString();
        if (strKey.equalsIgnoreCase("sessions")) {
            valueHashes.clear();
            return;
        }
        outputKey.set(strKey);
        outputValue.set("\n");
        oc.collect(outputKey, outputValue);
        while (values.hasNext()) {
            String v = values.next().toString();
            int hash = v.hashCode();
            if (!valueHashes.contains(hash)) {
                outputKey.set(v);
                outputValue.set("");
                oc.collect(outputKey, outputValue);
                valueHashes.clear();
                valueHashes.add(hash);
            }
        }
        valueHashes.clear();

    }
}
