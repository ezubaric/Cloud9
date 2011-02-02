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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.FloatWritable;
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

// reducer: sums up all the counts
public class PmiCombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

    private class SumMissing extends RuntimeException {
	public SumMissing(String message) {
	    // Constructor.  Create a ParseError object containing
	    // the given message as its error message.
	    super(message);
	}
    }
	
    // reuse objects
    private final static FloatWritable value = new FloatWritable();
    private int left_sum = 0;
    private int global_sum = 0;
    private String left;
    // private float marginal = -1.0f;
    // private String left = null;

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values,
		       Context context) throws IOException, InterruptedException {
	// sum up values	
	Iterator<FloatWritable> iter = values.iterator();
	int sum = 0;

	System.out.println(key.getLeftElement() + "," + key.getRightElement());
	// System.out.println("Sum was " + sum);

	while (iter.hasNext()) {
	    sum += iter.next().get();
	}

	if (key.getRightElement().equals("")) {
	    left = key.getLeftElement();
	    left_sum = sum;
	    value.set(sum);
	} else {
	    value.set(sum);
	}
	context.write(key, value);
    }
}
    
