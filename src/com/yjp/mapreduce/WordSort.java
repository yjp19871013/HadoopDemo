package com.yjp.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordSort {

	// 执行Map
	private static class WordSortMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
		
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
                	context.write(new Text(m.group(0)), new Text());
                }
            }
		}
	}
	
	// 执行Reduce
	private static class WordSortReducer 
		extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			context.write(key, new Text());
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordSort <inout path> <output path>");
			System.exit(-1);
		}
		
		// 设置类信息，方便hadoop从JAR文件中找到
		Job job = Job.getInstance();
		job.setJarByClass(WordSort.class);
		job.setJobName("Word Sort");
		
		// 添加输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 设置执行Map和Reduce的类
		job.setMapperClass(WordSortMapper.class);
		job.setReducerClass(WordSortReducer.class);
		
		//设置输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
