package com.yuan.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapperDemo extends Mapper<LongWritable,Text,Text,LongWritable> {

	private Text key;
	
	private LongWritable value;
	/**
	 * 
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		key = new Text();
		value = new LongWritable();
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String content = value.toString();
		if(content!=null&&content.contains("female")){
			String[] infos = content.split(",");
			this.key.set(infos[0]);
			this.value.set(Long.valueOf(infos[2]));
			context.write(this.key, this.value);
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
}
