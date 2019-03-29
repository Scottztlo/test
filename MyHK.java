package com.test.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyHK {
	// 实现 Mapper 
	public  static class MSMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable>{
		private  Text info = new Text();
		private IntWritable dayInfo = new IntWritable();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 处理单行文本 -- value
			String strLine = value.toString();
			String[] strArr = strLine.split(",");  
			String month = strArr[1];
			info.set(month);
			String dayStr = strArr[1];
			int dayInt = 0;
			try {
				dayInt = Integer.parseInt(dayStr);
			}
			catch (NumberFormatException e) {
				dayInt = 0;
			}
					
			dayInfo.set(dayInt);
			context.write(info, dayInfo);	
		}
		
	}
	
	// 实现 Reducer
	public static class MSReducer
		extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// 遍历相同 key 的链表（迭代器）
			int sum = 0;
			// 1. 获取迭代器
			Iterator<IntWritable> iterator = values.iterator();
			// 2. 遍历迭代器
			while(iterator.hasNext()) {
				IntWritable num = iterator.next();
				sum = sum + num.get();
			}
			IntWritable genderSum = new IntWritable(sum);
			// 3. 输出结果
			context.write(key, genderSum);
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 主控程序，设置 MR 参数和相关运行环境
		// 0. 依据 Configuration 创建一个 MR Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "My Score");
		job.setJarByClass(MyHK.class);
		
		// 1. 指定 job 输入的目录(s)
		FileInputFormat.setInputPaths(job, new Path(args[0]));		// 命令行参数的第一个参数作为输入目录
		job.setInputFormatClass(TextInputFormat.class);
		
		// 2. 指定 Mapper 类及其输出
		job.setMapperClass(MSMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 3. 指定 Reducer 类及其输出
		job.setReducerClass(MSReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 4. 指定输出信息
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 5. 提交job，投入执行
//		job.submit();
		job.waitForCompletion(true);
		boolean result = job.isSuccessful();
		System.exit(result ? 0 : 1);
		

	}


}
