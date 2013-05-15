/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dip.aol.query.clicks.models;

import edu.dip.aol.query.clicks.models.objects.RankedUrlWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author alex
 */
public class ComputeEStep {

    public static void main(String[] args) throws FileNotFoundException,
            IOException, Exception {
        new ComputeEStep().run(args);
    }

    public void run(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),
                new ComputeEStep.EStepJobsGenerator(), args);
        // new EStepJobsGenerator().run(args);
    }

    public static class Starter implements Runnable {

        private final JobControl jcontrol;

        public Starter(JobControl jcontrol) {
            this.jcontrol = jcontrol;
        }

        @Override
        public void run() {
            jcontrol.run();
        }
    }

    public class EStepJobsGenerator extends Configured implements Tool {

        public JobConf createParametersUpdateJob(Configuration conf,
                                                 FileSystem fs, Path in, Path out) throws IOException {
            JobConf jconf = new JobConf(conf, EStepJobsGenerator.class);
            if (fs.exists(out)) fs.delete(out, true);
            SequenceFileInputFormat.setInputPaths(jconf, in);
            SequenceFileOutputFormat.setOutputPath(jconf, out);
            jconf.setJobName("E step. parameters update job");

            jconf.setMapperClass(ParametersComputeMapper.class);
            jconf.setReducerClass(ParametersComputeReducer.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(SequenceFileOutputFormat.class);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(RankedUrlWritable.class);

            return jconf;
        }

        public JobConf createComputeMStepJob(Configuration conf, FileSystem fs,
                                             Path input, Path output) throws IOException {

            JobConf jconf = new JobConf(conf);

            if (fs.exists(output)) fs.delete(output, true);

            jconf.setMapperClass(ComputeMStep.ComputeDistributionsMapper.class);
            jconf.setReducerClass(ComputeMStep.ComputeDistributionsReducer.class);

            jconf.setInputFormat(SequenceFileInputFormat.class);
            jconf.setOutputFormat(SequenceFileOutputFormat.class);

            SequenceFileInputFormat.setInputPaths(jconf, input);
            SequenceFileOutputFormat.setOutputPath(jconf, output);

            jconf.setOutputKeyClass(Text.class);
            jconf.setOutputValueClass(RankedUrlWritable.class);

            jconf.setJobName("M step. Recompute Distribution Parameters");

            return jconf;
        }

        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Path inCompute = new Path(args[0]);
            Path outCompute = new Path(args[1]);
            Path inMStep = new Path(args[1]);
            Path outMStep = new Path(args[2]);

            FileSystem fs = FileSystem.get(conf);

            JobConf eStepJobConf = createParametersUpdateJob(conf, fs,
                    inCompute, outCompute);
            eStepJobConf.setJarByClass(ComputeEStep.class);
            Job eStepJob = new Job(eStepJobConf);
            JobConf mStepJobConf = createComputeMStepJob(conf, fs, inMStep,
                    outMStep);
            mStepJobConf.setJarByClass(ComputeEStep.class);
            Job mStepJob = new Job(mStepJobConf);
            JobControl jcontrol = new JobControl("compute");
            // JobClient.runJob(mStepJobConf);
            mStepJob.addDependingJob(eStepJob);
            jcontrol.addJob(eStepJob);
            jcontrol.addJob(mStepJob);
            Thread th = new Thread(new Starter(jcontrol));
            th.start();

            while (!jcontrol.allFinished()) {
                for (Job j : jcontrol.getRunningJobs()) {
                    System.out.println(j.getJobConf().getJobName() + "  "
                            + j.getMessage());
                }
                Thread.sleep(3000);
            }
            System.out.println("jobs finished");
            System.exit(1);

            return 0;
        }
    }
}
