package edu.dip.aol.query.clicks.models.statistics;

import edu.dip.aol.query.clicks.models.ComputeEStep;
import edu.dip.aol.query.clicks.models.objects.Pair;
import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class InitDistrParameters {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Main(), args);
//        new Main().run(args);
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        Path dir = new Path("/input/testUrlsFrequency");
//        Main.getHighestPartNumber(fs, dir);
    }

    private static class InitDistrComputeMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        private final Text outputKey = new Text();
        private final RankedUrlWritable outputValue = new RankedUrlWritable();

        @Override
        public void map(Text inputKey, RankedUrlWritable inputValue, OutputCollector<Text, RankedUrlWritable> oc, Reporter reporter) throws IOException {
            RankedUrlWritable wrurl = null;
            try {
                wrurl = RankedUrlWritable.copy(inputValue);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
            outputKey.set(wrurl.get().getUrl());
            outputValue.set(wrurl.get());
            oc.collect(outputKey, outputValue);
        }
    }

    private static class InitDistrComputeReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, Text> {
        @Override
        public void reduce(Text inputKey, Iterator<RankedUrlWritable> inputValues, OutputCollector<Text, Text> oc, Reporter reporter) throws IOException {
            int clickedUrls = 0;
            int totalUrls = 0;
            while (inputValues.hasNext()) {
                RankedUrlWritable wrurl = inputValues.next();
                if (wrurl.get().getClick() > 0) {
                    clickedUrls += 1;
                }
                totalUrls += 1;
            }
            oc.collect(new Text(inputKey.toString()), new Text(clickedUrls + " " + totalUrls));
        }
    }

    private static class InitUpdateRankedUrlsMapper extends MapReduceBase
            implements Mapper<Text, RankedUrlWritable, Text, RankedUrlWritable> {

        private final Hashtable<String, Pair> urlFrequencyBuffer = new Hashtable<String, Pair>();
        private final double error = 0.0001;

        @Override
        public void map(Text text, RankedUrlWritable rankedUrlWritable, OutputCollector<Text, RankedUrlWritable> textRankedUrlWritableOutputCollector, Reporter reporter) throws IOException {
            RankedUrlWritable wrurl = null;
            try {
                wrurl = RankedUrlWritable.copy(rankedUrlWritable);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return;
            }
            String url = wrurl.get().getUrl();
            Pair pair = urlFrequencyBuffer.get(url);
            if (pair == null) return;
            double attrDistribution = (double) pair.getV1() / (double) pair.getV2() + error;
            wrurl.get().setAttrDistributionFactor((float) attrDistribution);
            textRankedUrlWritableOutputCollector.collect(new Text(text.toString()), wrurl);
        }

        @Override
        public void configure(JobConf job) {
//            super.configure(job);
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    for (Path cacheFile : cacheFiles) {
                        if (cacheFile.toString().contains("part")) {
                            loadFrequencyBuffer(cacheFile);
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void loadFrequencyBuffer(Path file) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("[ \t]+");
                Pair pair = new Pair(Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
                urlFrequencyBuffer.put(parts[0], pair);
            }
        }
    }

    private static class InitUpdateRankedUrlsReducer extends MapReduceBase
            implements Reducer<Text, RankedUrlWritable, Text, RankedUrlWritable> {
        @Override
        public void reduce(Text text, Iterator<RankedUrlWritable> rankedUrlWritableIterator, OutputCollector<Text, RankedUrlWritable> textRankedUrlWritableOutputCollector, Reporter reporter) throws IOException {
            while (rankedUrlWritableIterator.hasNext()) {
                textRankedUrlWritableOutputCollector.collect(new Text(text.toString()), rankedUrlWritableIterator.next());
            }
        }
    }

    public static class Main extends Configured implements Tool {

        public static Path getHighestPartNumber(FileSystem fs, Path dir) throws IOException {
            Path finalPart = null;
            String fPath = "";
            for (FileStatus fileStatus : fs.listStatus(dir)) {
                String[] fileParts = fileStatus.getPath().toString().split("/");
                String filename = fileParts[fileParts.length - 1];
                if (filename.startsWith("part") && filename.compareTo(fPath) > 0) {
                    System.out.println(fileStatus.getPath().toString());
                    fPath = filename;
                    finalPart = fileStatus.getPath();
                }
            }
            return finalPart;
        }

        private JobConf getInitUpdateDisrtJob(Configuration conf, FileSystem fs, Path input, Path output) {

            JobConf jconf = new JobConf(conf);

            jconf.setMapperClass(InitUpdateRankedUrlsMapper.class);
            jconf.setReducerClass(InitUpdateRankedUrlsReducer.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(SequenceFileOutputFormat.class);

            SequenceFileInputFormat.setInputPaths(jconf, input);
            FileOutputFormat.setOutputPath(jconf, output);

            jconf.setJarByClass(InitDistrParameters.class);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(RankedUrlWritable.class);
            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(RankedUrlWritable.class);

            jconf.setJobName("Update Initial Distribution Parameters");
            return jconf;
        }

        private JobConf getInitDistrComputeJob(Configuration conf, FileSystem fs, Path input, Path output) {

            JobConf jconf = new JobConf(conf);

            jconf.setMapperClass(InitDistrComputeMapper.class);
            jconf.setReducerClass(InitDistrComputeReducer.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(TextOutputFormat.class);

            SequenceFileInputFormat.setInputPaths(jconf, input);
            FileOutputFormat.setOutputPath(jconf, output);

            jconf.setJarByClass(InitDistrParameters.class);

            jconf.setMapOutputKeyClass(Text.class);
            jconf.setMapOutputValueClass(RankedUrlWritable.class);
            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(Text.class);

            jconf.setJobName("Compute Initial Distribution Parameters");
            return jconf;
        }

        private void runAndWait(JobControl jcontrol) {

            Thread runner = new Thread(new ComputeEStep.Starter(jcontrol));
            runner.setPriority(Thread.MIN_PRIORITY);
            runner.start();

            while (!jcontrol.allFinished()) {
                for (Job job : jcontrol.getRunningJobs()) {
                    System.out.println(String.format("job name:  %s  job status: %s  ", job.getJobConf().getJobName(), job.getMessage()));
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }

        private void runJob(Configuration conf, FileSystem fs, JobConf jconf) throws IOException {

            Job idcJob = new Job(jconf);

            JobControl jcontrol = new JobControl("init distribution parameters");

            jcontrol.addJob(idcJob);
            runAndWait(jcontrol);
        }

        @Override
        public int run(String[] args) throws Exception {
            Path inputIDCPath = new Path(args[0]);
            Path outputIDCPath = new Path(args[1]);
            Path inputUpdatePath = new Path(args[2]);
            Path outputUpdatePath = new Path(args[3]);

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            JobConf idcJobConf = getInitDistrComputeJob(conf, fs, inputIDCPath, outputIDCPath);
            runJob(conf, fs, idcJobConf);

            Path urlsFreqFile = Main.getHighestPartNumber(fs, outputIDCPath);

            DistributedCache.addCacheFile(urlsFreqFile.toUri(), conf);

            JobConf idcUpdateJobConf = getInitUpdateDisrtJob(conf, fs, inputUpdatePath, outputUpdatePath);

            runJob(conf, fs, idcUpdateJobConf);

            System.exit(1);

            return 0;

        }

    }

}



