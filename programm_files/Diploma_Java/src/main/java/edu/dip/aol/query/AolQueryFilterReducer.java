/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
public class AolQueryFilterReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text();
    private final Text outputKey = new Text();
    private final int size = 30;
    private final Map<String, Collection<String>> userSessions = new HashMap<String, Collection<String>>();

    public void reduce(Text inputKey, Iterator<Text> inputValues, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
//        RankedUrl[] rurls = new RankedUrl[size];
//        while (inputValues.hasNext()) {
//            String inputValue = inputValues.next().toString();
//            String[] items = inputValue.split(" ");
//            if (items.length != 3) {
//                continue;
//            }
//            int rank = Integer.parseInt(items[1]);
//            if (rank >= size || rank <= 0) {
//                continue;
//            }
//            if (!userSessions.containsKey(items[0])) {
//                userSessions.put(items[0], new ArrayList<String>());
//            }
//            userSessions.get(items[0]).add(items[1]);
//            if (rurls[rank - 1] == null) {
//                rurls[rank - 1] = new RankedUrl(items[2], rank);
//            }
//        }
//        for (String uSession : userSessions.keySet()) {
//            Collection<String> items = userSessions.get(uSession);
//            for (int i = 0; i < rurls.length; i++) {
//                if (rurls[i] == null) {
//                    continue;
//                }
//                int ind = 0;
//                for (String item : items) {
//                    if (item.trim().equals((i + 1) + "")) {
//                        ind = 1;
//                        break;
//                    }
//                }
//                outputKey.set(uSession);
//                outputValue.set(String.format("%s [%s] %s %s", inputKey.toString(), (i + 1), rurls[i].getUrl(), ind));
//                oc.collect(outputKey, outputValue);
//            }
//            outputKey.set("");
//            outputValue.set("\n");
//            oc.collect(outputKey, outputValue);
//        }
//        userSessions.clear();
    }
}