package edu.dip.aol.query.clicks.models.simplified;

import edu.dip.aol.query.clicks.models.ComputeEStep;
import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: tierex
 * Date: 4/11/13
 * Time: 7:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimplifiedDBN {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Main(), args);
    }

    private static class ComputeInitialParametersMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {
        @Override
        public void map(Text text, RankedUrlWritable rankedUrlWritable, OutputCollector<Text, RankedUrlWritable> textRankedUrlWritableOutputCollector, Reporter reporter) throws IOException {
            try {
                String session = text.toString();
                textRankedUrlWritableOutputCollector.collect(new Text(session), RankedUrlWritable.copy(rankedUrlWritable));
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private static class ComputeInitialParametersReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, Text> {

        private final Hashtable<String, Collection<RankedUrlWritable>> localBuffer = new Hashtable<String, Collection<RankedUrlWritable>>();

        @Override
        public void reduce(Text text, Iterator<RankedUrlWritable> rankedUrlWritableIterator, OutputCollector<Text, Text> oc, Reporter reporter) throws IOException {
            String session = text.toString();

            if (!localBuffer.contains(session)) {
                localBuffer.put(session, new ArrayList<RankedUrlWritable>());
            }
            while (rankedUrlWritableIterator.hasNext()) {
                try {
                    RankedUrlWritable wrurl = RankedUrlWritable.copy(rankedUrlWritableIterator.next());
                    localBuffer.get(session).add(wrurl);
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace(System.out);
                    continue;
                }
            }
            for (String sess : localBuffer.keySet()) {
                int lastClickPosition = findLastClickPosition(localBuffer.get(sess));
                System.out.println("New session:  ");
                for (RankedUrlWritable wrurl : localBuffer.get(sess)) {
                    System.out.println(sess + "   " + wrurl.get().getUrl() + "   " + wrurl.get().getClick());
                }
                System.out.println("last click:  " + lastClickPosition);
                for (RankedUrlWritable wrurl : localBuffer.get(sess)) {
                    int ad = wrurl.get().getRank() <= lastClickPosition ? 1 : 0;
                    int an = wrurl.get().getClick() > 0 ? 1 : 0;
                    int sn = wrurl.get().getRank() == lastClickPosition ? 1 : 0;
                    int sd = wrurl.get().getClick() > 0 ? 1 : 0;

                    oc.collect(new Text(wrurl.get().getUrl()), new Text(String.format("%s %s %s %s", ad, an, sd, sn)));
                }
            }

            localBuffer.clear();

        }

        private int findLastClickPosition(Collection<RankedUrlWritable> wrurls) {
            int position = 0;
            for (RankedUrlWritable wrurl : wrurls) {
                if (wrurl.get().getClick() > 0 && position < wrurl.get().getRank()) {
                    position = wrurl.get().getRank();
                }
            }
            return position;
        }
    }

    private static class CollectParametersMapper extends MapReduceBase
            implements Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
            textTextOutputCollector.collect(key, value);
        }
    }

    private static class CollectParametersReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
            int ad = 0, an = 0, sd = 0, sn = 0;
            String url = text.toString();
            while (textIterator.hasNext()) {
                String[] strValues = textIterator.next().toString().split("[ \t]+");
                ad += Integer.parseInt(strValues[0]);
                an += Integer.parseInt(strValues[1]);
                sd += Integer.parseInt(strValues[2]);
                sn += Integer.parseInt(strValues[3]);
            }
            textTextOutputCollector.collect(new Text(url), new Text(String.format("%s %s %s %s", ad, an, sd, sn)));
        }
    }

    private static class ComputeFinalParametersMapper extends MapReduceBase
            implements Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text inputKey, Text inputValue, OutputCollector<Text, Text> oc, Reporter reporter) throws IOException {
            String url = inputKey.toString();
            String[] vParts = inputValue.toString().split("[ \t]+");
            int ad = Integer.parseInt(vParts[0]);
            int an = Integer.parseInt(vParts[1]);
            int sd = Integer.parseInt(vParts[2]);
            int sn = Integer.parseInt(vParts[3]);

            double au = ((double) an + 0.001) / ((double) ad + 0.001);
            double su = ((double) sn + 0.001) / ((double) sd + 0.001);
            double r = au * su;
            oc.collect(new Text(url), new Text(String.format("%s %s %s", au, su, r)));
        }
    }

    private static class ComputeFinalParametersReducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {
            while (textIterator.hasNext()) {
                textTextOutputCollector.collect(text, textIterator.next());
            }
        }
    }

    public static class Main extends Configured implements Tool {

        private JobConf createInitialParametersComputeJob(Configuration conf, FileSystem fs, Path input, Path output) throws IOException {

            if (fs.exists(output)) fs.delete(output, true);

            JobConf jconf = new JobConf(conf);
            jconf.setJobName("1. Initial Parameters Compute");

            SequenceFileInputFormat.addInputPath(jconf, input);
            FileOutputFormat.setOutputPath(jconf, output);

            jconf.setMapperClass(ComputeInitialParametersMapper.class);
            jconf.setReducerClass(ComputeInitialParametersReducer.class);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(RankedUrlWritable.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            return jconf;
        }

        private JobConf createCollectParametersJob(Configuration conf, FileSystem fs, Path input, Path output) throws IOException {

            if (fs.exists(output)) fs.delete(output, true);

            JobConf jconf = new JobConf(conf);
            jconf.setJobName("2. Collect Parameters");

            FileInputFormat.addInputPath(jconf, input);
            FileOutputFormat.setOutputPath(jconf, output);

            jconf.setMapperClass(CollectParametersMapper.class);
            jconf.setReducerClass(CollectParametersReducer.class);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(Text.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.setInputFormat(KeyValueTextInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            return jconf;
        }

        private JobConf createComputeFinalParametersJob(Configuration conf, FileSystem fs, Path input, Path output) throws IOException {

            if (fs.exists(output)) fs.delete(output, true);

            JobConf jconf = new JobConf(conf);
            jconf.setJobName("3. Compute Final Parameters");

            FileInputFormat.addInputPath(jconf, input);
            FileOutputFormat.setOutputPath(jconf, output);

            jconf.setMapperClass(ComputeFinalParametersMapper.class);
            jconf.setReducerClass(ComputeFinalParametersReducer.class);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(Text.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.setInputFormat(KeyValueTextInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            return jconf;
        }

        private void runAndWait(JobControl jcontrol) {

            Thread runner = new Thread(new ComputeEStep.Starter(jcontrol));
            runner.setPriority(Thread.MIN_PRIORITY);
            runner.start();

            while (!jcontrol.allFinished()) {
                for (Job job : jcontrol.getRunningJobs()) {
                    System.out.println(String.format("job name:  %s ;  job status: %s  ", job.getJobConf().getJobName(), job.getMessage()));
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(conf);

            Path IPinputPath = new Path(args[0]);
            Path IPoutputPath = new Path(args[1]);

            Path CPinputPath = new Path(args[1]);
            Path CPoutputPath = new Path(args[2]);

            Path FPinputPath = new Path(args[2]);
            Path FPoutputPath = new Path(args[3]);

            JobConf ipJobConf = createInitialParametersComputeJob(conf, fs, IPinputPath, IPoutputPath);
            JobConf cpJobConf = createCollectParametersJob(conf, fs, CPinputPath, CPoutputPath);
            JobConf fpJobConf = createComputeFinalParametersJob(conf, fs, FPinputPath, FPoutputPath);

            ipJobConf.setJarByClass(SimplifiedDBN.class);
            cpJobConf.setJarByClass(SimplifiedDBN.class);
            fpJobConf.setJarByClass(SimplifiedDBN.class);

            Job ipJob = new Job(ipJobConf);
            Job cpJob = new Job(cpJobConf);
            Job fpJob = new Job(fpJobConf);

            fpJob.addDependingJob(cpJob);
            cpJob.addDependingJob(ipJob);

            JobControl jcontrol = new JobControl("simplified DBN algorithm");

            jcontrol.addJob(ipJob);
            jcontrol.addJob(cpJob);
            jcontrol.addJob(fpJob);

            runAndWait(jcontrol);

            System.exit(1);

            return 1;
        }
    }


}
