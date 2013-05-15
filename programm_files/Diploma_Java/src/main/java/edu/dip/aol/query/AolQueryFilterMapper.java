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
 public class AolQueryFilterMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
            String strValue = value.toString().trim();
            if (strValue.isEmpty() || strValue.equalsIgnoreCase("\n")) {
                return;
            }
            String[] pInput = parseInput(strValue);
            outputKey.set(pInput[1]);
            outputValue.set(String.format("%s %s %s", pInput[0], pInput[2], pInput[3]));
            oc.collect(outputKey, outputValue);
        }

        private String[] parseInput(String input) {
            String[] its = input.trim().split("\t");
            String[] outValue = its[1].split(" +");
            return new String[]{its[0], outValue[0],
                        outValue[1].substring(1, outValue[1].length() - 1), outValue[2]};
        }
    }
