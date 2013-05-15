package edu.dip.aol.query.clicks.models.simplified;

import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import edu.dip.aol.query.clicks.models.objects.WeightedUrl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: tierex
 * Date: 4/18/13
 * Time: 2:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class MergeRelevance {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Main(), args);
    }

    private static class Main extends Configured implements Tool {
        @Override
        public int run(String[] args) throws Exception {
            Path cacheFile = new Path(args[0]);
            Path inputPath = new Path(args[1]);
            Path outputPath = new Path(args[2]);

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            DistributedCache.addCacheFile(cacheFile.toUri(), conf);

            if (fs.exists(outputPath)) fs.delete(outputPath, true);

            JobConf jconf = new JobConf(conf);

            jconf.setJarByClass(MergeRelevance.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            SequenceFileInputFormat.addInputPath(jconf, inputPath);
            FileOutputFormat.setOutputPath(jconf, outputPath);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(RankedUrlWritable.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.setMapperClass(MergeMapper.class);
            jconf.setReducerClass(MergeReducer.class);

            JobClient.runJob(jconf);

            return 0;
        }
    }

    private static class MergeMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        private final HashMap<String, WeightedUrl> localBuffer = new HashMap<String, WeightedUrl>();

        @Override
        public void map(Text inputKey, RankedUrlWritable inputValue, OutputCollector<Text, RankedUrlWritable> oc, Reporter reporter) throws IOException {
            String session = inputKey.toString().trim();
            RankedUrlWritable wrurl = null;
            try {
                wrurl = RankedUrlWritable.copy(inputValue);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            WeightedUrl weUrl = localBuffer.get(wrurl.get().getUrl().trim());
            if (weUrl == null) return;
            wrurl.get().setAttractivness((float) weUrl.getFinalAttractivness());
            wrurl.get().setSatisfaction((float) weUrl.getFinalSatisfaction());
            wrurl.get().setAttrDistributionFactor((float) weUrl.getFinalRelevance());
            oc.collect(new Text(session), wrurl);
        }

        @Override
        public void configure(JobConf job) {
            //super.configure(job);
            Path[] cacheFiles = null;
            try {
                cacheFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            if (cacheFiles == null || cacheFiles.length == 0) return;
            String line = "";
            try {
                BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("[ \t]+");
                    for (int i = 0; i < parts.length; ++i) {
                        System.out.print(parts[i] + "   ");
                    }
                    System.out.println();
//                    if (parts.length != 4) continue;
                    String url = parts[0];
                    WeightedUrl weUrl = new WeightedUrl(url, Double.parseDouble(parts[1]), Double.parseDouble(parts[2]), Double.parseDouble(parts[3]));
                    localBuffer.put(url, weUrl);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class MergeReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, Text> {
        @Override
        public void reduce(Text inputKey, Iterator<RankedUrlWritable> inputValues, OutputCollector<Text, Text> oc, Reporter reporter) throws IOException {
            while (inputValues.hasNext()) {
                RankedUrlWritable wrurl = inputValues.next();
                String text = wrurl.get().getUrl() + "   " + wrurl.get().getQuery() + "  " + wrurl.get().getRank() + "   [" + wrurl.get().getAttractivness() + "  " + wrurl.get().getSatisfaction() + "  " + wrurl.get().getAttrDistributionFactor() + "]";
                oc.collect(inputKey, new Text(text));
            }
            oc.collect(new Text("\n"), new Text(""));
        }
    }
}
