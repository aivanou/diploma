/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models;

import edu.dip.aol.query.clicks.models.objects.RankedUrl;
import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class ParametersComputeReducer extends MapReduceBase
        implements Reducer<Text, RankedUrlWritable, Text, RankedUrlWritable> {

    private final Map<String, Collection<RankedUrlWritable>> localBuffer = new HashMap<String, Collection<RankedUrlWritable>>();

    public void reduce(Text inputKey, Iterator<RankedUrlWritable> values, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
        double attractivness = 0.0;
        double neqAttractivness = 0.0;
        double satisfaction = 0.0;
        double neqSatisfaction = 0.0;

        while (values.hasNext()) {
            RankedUrlWritable w = values.next();
            RankedUrl rurl = w.get();
            attractivness += rurl.getAttrDistributionFactor();
            neqAttractivness += 1 - rurl.getAttrDistributionFactor();
            if (rurl.getClick() > 0) {
                neqSatisfaction += 1 - rurl.getSatDistributionFactor();
                satisfaction += rurl.getSatDistributionFactor();
            }
            if (!localBuffer.containsKey(rurl.getSession())) {
                localBuffer.put(rurl.getSession(), new ArrayList<RankedUrlWritable>());
            }
            try {
                localBuffer.get(rurl.getSession()).add(RankedUrlWritable.copy(w));
            } catch (CloneNotSupportedException ex) {
                Logger.getLogger(ParametersComputeReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        double attr = (double) attractivness / (double) (attractivness + neqAttractivness);
        double satt = (double) satisfaction / (double) (satisfaction + neqSatisfaction);
        for (String key : localBuffer.keySet()) {
            for (RankedUrlWritable value : localBuffer.get(key)) {
                value.get().setSession(key);
                value.get().setAttractivness((float) attr);
                value.get().setSatisfaction((float) satt);
                RankedUrlWritable outputValue = new RankedUrlWritable();
                oc.collect(new Text(key), value);
            }
        }
        localBuffer.clear();
    }
}
