package com.yjp.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCount {

	// 执行Map
	private static class CharCountMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();  
            StringTokenizer token = new StringTokenizer(line);
            
            while (token.hasMoreTokens()) {
            	String word = token.nextToken();
            	Pattern p = Pattern.compile("[A-Za-z]+");
                Matcher m = p.matcher(word);
                if (m.find()) {
                	word = m.group(0);
	            	for (int i = 0; i < word.length(); i++) {
	            		context.write(new Text(word.charAt(i) + ""), one);
	            	}
                }
            }
		}
	}
	
	// 执行Reduce
	private static class CharCountReducer 
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: CharCount <inout path> <output path>");
			System.exit(-1);
		}
		
		// 设置类信息，方便hadoop从JAR文件中找到
		Job job = Job.getInstance();
		job.setJarByClass(CharCount.class);
		job.setJobName("Char Count");
		
		// 添加输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 设置执行Map和Reduce的类
		job.setMapperClass(CharCountMapper.class);
		job.setReducerClass(CharCountReducer.class);
		
		//设置输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
