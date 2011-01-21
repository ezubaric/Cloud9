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

package edu.umd.cloud9.example.translation;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

import edu.umd.cloud9.io.PairOfStrings;

/**
 * <p>
 * Simple word count demo. This Hadoop Tool counts words in flat text file, and
 * takes the following command-line arguments:
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
public class TranslationProbability extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(TranslationProbability.class);


    /**
     * Creates an instance of this tool.
     */
    public TranslationProbability() {
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
	
	sLogger.info("Tool: TranslationProbability");
	sLogger.info(" - input path: " + inputPath);
	sLogger.info(" - output path: " + outputPath);
	sLogger.info(" - number of reducers: " + reduceTasks);
	
	Configuration conf = new Configuration();
	Job job = new Job(conf, "TranslationProbability");
	job.setJarByClass(TranslationProbability.class);
	
	job.setNumReduceTasks(reduceTasks);
	
	FileInputFormat.setInputPaths(job, new Path(inputPath));
	FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
	job.setOutputKeyClass(PairOfStrings.class);
	job.setOutputValueClass(FloatWritable.class);
	
	job.setMapperClass(TransProbMapper.class);
	job.setPartitionerClass(TransProbPartitioner.class);
	// job.setCombinerClass(TransProbReducer.class);
	job.setReducerClass(TransProbReducer.class);

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
	int res = ToolRunner.run(new Configuration(), new TranslationProbability(), args);
	System.exit(res);
    }
}
