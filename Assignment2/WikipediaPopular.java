// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String timestamp = itr.nextToken(), lang = itr.nextToken(), title = itr.nextToken();
            long view_count = Long.parseLong(itr.nextToken());
            String bytes_returned = itr.nextToken();

            if(lang.equals("en") && (!title.equals("Main_Page") && !title.startsWith("Special:"))){
                word.set(timestamp);
                context.write(word, new LongWritable(view_count));
            }
		}
	}

	public static class MaxReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {

			long maxCount = Integer.MIN_VALUE;
			for (LongWritable value : values) {
				maxCount = value.get() > maxCount ? value.get() : maxCount;
			}
			result.set(maxCount);
			context.write(key, result);
		}
	}

	// public static class Combiner
	// extends Reducer<Text, LongWritable, Text, LongWritable> {
	// 	private LongWritable result = new LongWritable();

	// 	@Override
	// 	public void reduce(Text key, Iterable<LongWritable> values,
	// 			Context context
	// 			) throws IOException, InterruptedException {
    //         long maxCount = Integer.MIN_VALUE;
	// 		for (LongWritable value : values) {
	// 			maxCount = value.get() > maxCount ? value.get() : maxCount;
	// 		}
	// 		result.set(maxCount);
	// 		context.write(key, result);
	// 	}
	// }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}