package hw5part3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Part3 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "part3");
		job.setJarByClass(Part3.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(IPAndDate.class);
		job.setOutputValueClass(IntWritable.class);
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Job job2 = Job.getInstance(new Configuration(), "part3");
		job2.setJarByClass(Part3.class);
		job2.setMapperClass(MySecondMapper.class);
		job2.setReducerClass(MySecondReducer.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		ctrljob2.addDependingJob(ctrljob1);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		JobControl jobCtrl = new JobControl("myOutCount");

		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);

		Thread t = new Thread(jobCtrl);
		t.start();
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
