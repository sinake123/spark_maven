package com.yuan.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.yuan.constant.MRParam;
import com.yuan.util.HdfsUtil;

public class ReducerDemo extends Reducer<Text, LongWritable, Text, Text> {

	private String outPath;
	private FileSystem fileSystem;
	private String fileName;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		outPath = conf.get(MRParam.OUTPUT_PATH);
		fileSystem = FileSystem.get(conf);
		fileName = outPath+File.separator+"demo.txt";
	}
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException,
			InterruptedException {
		long time = 0;
		for(Iterator<LongWritable> it = values.iterator();it.hasNext();){
			time+=it.next().get();
		}
		String content = key.toString()+":"+time;
		HdfsUtil.createOrAppendToFile(fileName, content, fileSystem);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		fileSystem.close();
	}
	
	
}
