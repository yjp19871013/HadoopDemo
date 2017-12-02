package com.yjp.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSEcho {
	
	// yjp替换为你的用户名
	private static final URI TMP_URI = 
			URI.create("hdfs://localhost/user/yjp/temp");

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Usage: HDFSEcho string");
			System.exit(1);
		}
		
		String echoString = args[0] + "\n";
		
		// 通过URI创建FileSystem，由于是hdfs，会返回代表HDFS的实例
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(TMP_URI, conf);
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		
		try {
			// 写入文件
			Path path = new Path(TMP_URI);
			out = fs.create(path);
			out.writeUTF(echoString);
			IOUtils.closeStream(out);
			
			//从文件读取并输出
			in = fs.open(path);
			IOUtils.copyBytes(in, System.out, conf);
			IOUtils.closeStream(in);
			
			//删除文件
			fs.delete(path, true);
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}
	}
	
}
