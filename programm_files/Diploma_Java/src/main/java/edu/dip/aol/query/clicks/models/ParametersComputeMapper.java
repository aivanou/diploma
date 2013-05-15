/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models;

import edu.dip.aol.query.clicks.models.objects.UserSessionWritable;
import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author alex
 */
public class ParametersComputeMapper extends MapReduceBase
        implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {

    public void map(Text key, RankedUrlWritable value, OutputCollector<Text, RankedUrlWritable> oc, Reporter rprtr) throws IOException {
        RankedUrlWritable out = new RankedUrlWritable();
        out.set(value.get());
        oc.collect(new Text(out.get().getUrl()), out);

        //        for (RankedUrlWritable rurl : value.get().getRurls()) {
//            String url = rurl.get().getUrl();
//            oc.collect(new Text(url.trim().toLowerCase()), rurl);
//        }
    }
}
