/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.example.pmi;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * <p>
 * Totals the number of entities, delimited by brackets, in a Wackypedia dump.
 * </p>
 *
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 *
 * @author Jordan Boyd-Graber
 */
public class EntityCounter extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(EntityCounter.class);

	// mapper: emits (token, 1) for every word occurrence
	private static class EntityCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// reuse objects to save overhead of object creation
		private final static IntWritable one = new IntWritable(1);
		private Text entity = new Text();
		private Text empty_entity = new Text("");

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException {
      Iterator nps = new BracketResults(((Text) value).toString());
			while (nps.hasNext()) {
				entity.set((String)nps.next());
				context.write(entity, one);

				// And a dummy entity so we can normalize
				context.write(empty_entity, one);
			}
		}
	}

	// reducer: sums up all the counts
	private static class EntityCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// sum up values
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SumValue.set(sum);
      if (sum > PmiReducer.MIN_ENTITY_COUNT)
        context.write(key, SumValue);
		}
	}

  	// reducer: sums up all the counts
	private static class EntityCounterCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		// reuse objects
		private final static IntWritable SumValue = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// sum up values
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SumValue.set(sum);
			context.write(key, SumValue);
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public EntityCounter() {
	}

	private static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);

		sLogger.info("Tool: EntityCounter");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of reducers: " + reduceTasks);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "EntityCounter");
		job.setJarByClass(EntityCounter.class);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(EntityCounterMapper.class);
		job.setCombinerClass(EntityCounterCombiner.class);
		job.setReducerClass(EntityCounterReducer.class);

		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EntityCounter(), args);
		System.exit(res);
	}
}
