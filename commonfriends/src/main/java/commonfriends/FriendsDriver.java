package commonfriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FriendsDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException{
		Path input_dir= new Path("hdfs://localhost:9000/friends_data_in");
		Path output_dir= new Path("hdfs://localhost:9000/friends_data_out");
		
		Configuration conf = new Configuration();
		
		Job job=Job.getInstance(conf,"MyFriendsJob");
		
		job.setJarByClass(FriendsDriver.class);
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job,input_dir);
		FileOutputFormat.setOutputPath(job,output_dir);
		
		output_dir.getFileSystem(conf).delete(output_dir,true);
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
		
	}

}
