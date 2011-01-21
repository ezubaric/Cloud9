package edu.umd.cloud9.example.translation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.umd.cloud9.io.PairOfStrings;

public class TransProbPartitioner extends Partitioner
					  <PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value,
			    int numReduceTasks) {
	return (key.getLeftElement().hashCode() & Integer.MAX_VALUE)
	    % numReduceTasks;
    }
}