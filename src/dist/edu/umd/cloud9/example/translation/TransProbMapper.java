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


// mapper: emits (token_pair, 1) for every translation pair
public class TransProbMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

    // reuse objects to save overhead of object creation
    private final static FloatWritable one = new FloatWritable(1);
    private static final PairOfStrings pair = new PairOfStrings();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
	InterruptedException {
	String line = ((Text) value).toString();
	StringTokenizer itr = new StringTokenizer(line);
	System.out.println("--------------------------------");
	while (itr.hasMoreTokens()) {
	    String piece = itr.nextToken();
	    String[] token = piece.split("::", 2);
	    if (token.length < 2) continue;
	    
	    pair.set(token[0], token[1]);
	    context.write(pair, one);
	    
	    pair.set(token[0], "");
	    context.write(pair, one);

	    if(token[0].compareTo("") == 0)
		System.out.println("~" + piece + "|" + token[0] + "|" +
				   token[1] + "~");
	}
    }
}
