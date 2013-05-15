/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class AolReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    Map<String, Integer> urlsClicks = new HashMap<String, Integer>();

    public void reduce(Text inputKey, Iterator<Text> inputValues, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
//        String query = inputKey.toString();
//        if (query.trim().equals("-")) {
//            inputKey.set(query);
//            return;
//        }
//        while (inputValues.hasNext()) {
//            Text v = inputValues.next();
//            if (v == null) {
//                continue;
//            }
//            String value = v.toString().trim();
//            if (value.isEmpty()) {
//                continue;
//            }
//            if (!urlsClicks.containsKey(value)) {
//                urlsClicks.put(value, 1);
//                continue;
//            }
//            urlsClicks.put(value, urlsClicks.get(value) + 1);
//        }
//        RankedUrl[] rurls = new RankedUrl[100];
//        for (int i = 0; i < 100; i++) {
//            rurls[i] = null;
//        }
//        for (String key : urlsClicks.keySet()) {
//            String[] items = key.split(" ");
//            if (items.length != 2) {
//                continue;
//            }
//            RankedUrl rurl = new RankedUrl(items[1], Integer.parseInt(items[0]));
//            rurl.setClicks(urlsClicks.get(key));
//            if (rurl.getRank() < 99 && rurls[rurl.getRank()] == null) {
//                rurls[rurl.getRank()] = rurl;
//            }
//        }
//        int clicks = 0;
//        for (RankedUrl rurl : rurls) {
//            if (rurl == null) {
//                continue;
//            }
//            clicks += rurl.getClicks();
//        }
//        if (clicks > 1) {
//            StringBuilder output = new StringBuilder();
//            for (int i = 1; i < rurls.length; i++) {
//                if (rurls[i] == null) {
//                    continue;
//                }
//                output.append(rurls[i].toString()).append("\n");
//            }
//            outputKey.set("\nquery:  " + query + "\n");
//            outputValue.set(output.toString());
//            oc.collect(outputKey, outputValue);
//        }
//        outputValue.set("");
//        urlsClicks.clear();
    }
}
