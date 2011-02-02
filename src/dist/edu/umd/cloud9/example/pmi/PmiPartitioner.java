package edu.umd.cloud9.example.pmi;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.umd.cloud9.io.PairOfStrings;

public class PmiPartitioner extends Partitioner
					  <PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value,
			    int numReduceTasks) {
	return (key.getRightElement().hashCode() & Integer.MAX_VALUE)
	    % numReduceTasks;
    }
}