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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;

/**
 *
 * @author alex
 */
public class ComputeMStep {

    public static class ComputeDistributionsMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        public void map(Text key, RankedUrlWritable value, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
            oc.collect(key, value);
        }
    }

    public static class ComputeDistributionsReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        private Map<String, List<RankedUrlWritable>> buffer = new HashMap<String, List<RankedUrlWritable>>();
        private final double gamma = 0.9;

        public void reduce(Text key, Iterator<RankedUrlWritable> values, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
            String strKey = key.toString();
            while (values.hasNext()) {
                RankedUrlWritable w = values.next();
                if (!buffer.containsKey(strKey)) {
                    buffer.put(strKey, new ArrayList<RankedUrlWritable>());
                }
                try {
                    buffer.get(strKey).add(RankedUrlWritable.copy(w));
                } catch (CloneNotSupportedException ex) {
                    Logger.getLogger(ComputeMStep.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            for (String session : buffer.keySet()) {
                System.out.println("processing session:  " + session);
                List<RankedUrlWritable> wrurls = buffer.get(session);
                Collections.sort(wrurls, new Comparator<RankedUrlWritable>() {
                    @Override
                    public int compare(RankedUrlWritable o1, RankedUrlWritable o2) {
                        if (o1.get().getRank() == o2.get().getRank()) {
                            return 0;
                        }
                        return o1.get().getRank() > o2.get().getRank() ? 1 : -1;
                        //                        return Integer.compare(o1.get().getRank(), o2.get().getRank());
                    }
                });
                int lastClickedUrl = findLastClickedUrl(wrurls);
                double[] alphas = new double[wrurls.size()];
                double[] betas = new double[wrurls.size()];
                alphas[0] = 1.0;
                betas[wrurls.size() - 1] = 1.0;
                for (int i = 1; i < wrurls.size(); i++) {
                    double au = wrurls.get(i).get().getAttractivness();
                    double su = wrurls.get(i).get().getSatisfaction();
                    boolean isClicked = wrurls.get(i).get().getClick() > 0;
                    //e'=0
                    double p_e1c_e0 = 0.0;
                    //e'=1
                    double p_e1c_e1;
                    if (isClicked) {
                        p_e1c_e1 = gamma * (1 - su) * au;
                    } else {
                        p_e1c_e1 = gamma * 1 * au;
                    }
                    alphas[i] = alphas[i - 1] * p_e1c_e1;
                    System.out.println(String.format("alphas[%s] = %f", i, alphas[i]));
                }
                for (int i = wrurls.size() - 2; i >= 0; --i) {
                    double au = wrurls.get(i).get().getAttractivness();
                    double su = wrurls.get(i).get().getSatisfaction();
                    boolean isClicked = wrurls.get(i).get().getClick() > 0;
                    //e'=0
                    double p_e0c_e1 = 0.0;
                    //e'=1
                    double p_e1c_e1 = 0.0;
                    if (isClicked) {
                        p_e0c_e1 = (1 - gamma) * (1 - su) * au + 1 * au * su;
                        p_e1c_e1 = gamma * (1 - su) * au;
                    } else {
                        p_e0c_e1 = (1 - gamma) * 1 * au;
                        p_e1c_e1 = gamma * 1 * au;
                    }
                    betas[i] = betas[i + 1] * p_e1c_e1 + (1.0 - betas[i + 1]) * p_e0c_e1;
                    System.out.println(String.format("betas[%s] = %f", i, betas[i]));
                }
                for (int i = 0; i < wrurls.size(); i++) {
                    double attrDistrFactor = wrurls.get(i).get().getAttrDistributionFactor() * alphas[i] * betas[i];
                    double satDistrFactor = wrurls.get(i).get().getSatDistributionFactor() * alphas[i] * betas[i];
                    attrDistrFactor = attrDistrFactor / alphas[alphas.length - 1];
                    satDistrFactor = satDistrFactor / alphas[alphas.length - 1];
                    System.out.println(String.format("attrd [%s] = %f", i, (float) (attrDistrFactor)));
                    System.out.println(String.format("satisd [%s] = %f", i, (float) satDistrFactor));
                    wrurls.get(i).get().setAttrDistributionFactor((float) (attrDistrFactor));
                    wrurls.get(i).get().setSatDistributionFactor((float) satDistrFactor);
                }
                for (RankedUrlWritable wrurl : wrurls) {
                    oc.collect(new Text(wrurl.get().getSession()), wrurl);
                }
            }
            buffer.clear();
        }

        private int findLastClickedUrl(List<RankedUrlWritable> wrurls) {
            int index = -1;
            for (int i = 0; i < wrurls.size(); i++) {
                if (wrurls.get(i).get().getClick() > 0) {
                    index = i;
                }
            }
            return index;
        }
    }
}
