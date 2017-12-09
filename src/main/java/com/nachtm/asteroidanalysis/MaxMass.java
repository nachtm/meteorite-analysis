import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

public class MaxMass {

	public static void main(String[] args) throws Exception {
		if (args.length != 2){
			System.err.println("Usage: MaxMass input output");
			System.exit(1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(MaxMass.class);
		job.setJobName("Max Mass");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaxMassMapper.class);
		job.setReducerClass(MaxMassReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}