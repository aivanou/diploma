/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query;

import edu.dip.aol.query.clicks.models.objects.RankedUrl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class AolPrettyReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
//    private final Map<String, RankedUrl[]> queryMap = new HashMap<String, RankedUrl[]>();
    private final int maxRank = 20;

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> oc, Reporter rpr) throws IOException {
//        while (values.hasNext()) {
//            Text value = values.next();
//            String[] items = value.toString().split(" ");
//            if (items.length != 3) {
//                continue;
//            }
//            int rank = Integer.parseInt(items[1]);
//            if (rank >= maxRank) {
//                continue;
//            }
//            String query = items[0];
//            if (!queryMap.containsKey(query)) {
//                queryMap.put(query, new RankedUrl[maxRank]);
//            }
//            RankedUrl rurl = new RankedUrl(items[2], rank);
//            queryMap.get(query)[rank] = rurl;
//        }
//        int sessionAdder = 1;
//        for (String query : queryMap.keySet()) {
//            String newSession = key.toString() + ":" + ++sessionAdder;
//            RankedUrl[] rurls = queryMap.get(query);
//            removeDublicateUrls(rurls);
//            for (int i = 0; i < rurls.length; i++) {
//                if (rurls[i] == null) {
//                    continue;
//                }
//                outputKey.set(newSession);
//                outputValue.set(query + " " + rurls[i].toString());
//                oc.collect(outputKey, outputValue);
//            }
//            outputKey.set("");
//            outputValue.set("\n");
//            oc.collect(outputKey, outputValue);
//        }
//        queryMap.clear();
    }

    private void removeDublicateUrls(RankedUrl[] rurls) {
//        for (int i = 0; i < rurls.length; i++) {
//            if (rurls[i] == null) {
//                continue;
//            }
//            for (int j = i + 1; j < rurls.length; j++) {
//                if (rurls[j] == null) {
//                    continue;
//                }
//                if (rurls[i].getUrl().equalsIgnoreCase(rurls[j].getUrl())) {
//                    rurls[j] = null;
//                }
//            }
//        }
    }
}
