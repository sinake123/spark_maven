package com.yuan.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
import com.yuan.constant.MRParam;
import com.yuan.util.HdfsUtil;

public class Start {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String path = args[0];
		String inputPath = path + File.separator + "input";
		String outputPath = path + File.separator + "output";
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		FileStatus[] fileStatus = HdfsUtil.listStatus(inputPath, fileSystem);
		fileSystem.close();
		int length = fileStatus==null?0:fileStatus.length;
		JobConf jobConf = new JobConf(conf);
		jobConf.set(MRParam.OUTPUT_PATH, outputPath);
		Job job = Job.getInstance(conf,"demo");
		job.setJarByClass(Start.class);
		job.setMapperClass(MapperDemo.class);
		job.setReducerClass(ReducerDemo.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); 
		job.setNumReduceTasks(length/4+1);
		
		FileInputFormat.addInputPath(jobConf, new Path(inputPath));
		FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
		job.waitForCompletion(true);
	}
}
