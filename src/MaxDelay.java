//Copyright (c) 2015 Elena Rose, University of Tampere
//
//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in
//all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//THE SOFTWARE.

// maxDelay application finds the maximum delay for each bus line in the bus movements csv file

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxDelay {	
	
	public static class Map extends	Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line =value.toString();
			String[] myStrings = line.split(",");
			String lineNo = myStrings[1];
			int delay = 901; //15 minutes or 900 seconds by default
			if (!myStrings[0].equals("time")) {//skip the header of the file
				delay = Integer.parseInt(myStrings[8]); //delay
				if (delay > -901 && delay < 901) //skip outliers more than |900| seconds
					context.write(new Text(lineNo), new IntWritable(delay));
			}	
		}
	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)  
				throws IOException, InterruptedException {
			
			int maxDelay = 0;
			for(IntWritable value:values){
				maxDelay = Math.max(maxDelay, value.get());
			}
			
			context.write(key, new IntWritable(maxDelay));		 
		}

	}
	
	public static void main(String[] args) throws Exception { 
		
		if (args.length != 2) {           //minimum testing for arguments
			System.err.println("Usage: maxDelay <input path> <output path>"); 
			System.exit(-1);
		}
		
		//clean the former output if it exists
		Path p = new Path(args[1]);
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(p)) {
		   	fs.delete(p, true);
		}
			    
		//run a job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find maximum delays");
		job.setJarByClass(MaxDelay.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				 
		// describe the input and output specification for the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1); //launch the job and return success or not
	}
	
}
