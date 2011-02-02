package edu.umd.cloud9.example.pmi;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

public class RawReader {

    private BufferedReader reader;
    private String line;
    private FileSystem hdfs;

    public void reset(String filename) {
	try {
	    hdfs = FileSystem.get(new Configuration());
	  
	    Configuration conf = new Configuration(true);
	    FileSystem fs = FileSystem.get(conf);
	    Path path = new Path(filename);
	    FSDataInputStream dis = hdfs.open(path);
	    reader = new BufferedReader(new InputStreamReader(dis));
	}catch ( IOException e ) {
	    System.out.println("Usage dirc <directory> ");
	    return ;
	} catch (ArrayIndexOutOfBoundsException e) {
	    System.out.println("Usage dirc <directory> ");
	    return ;
	}
	line = next();
    }

    public RawReader(String dirname) {
	reset(dirname);
    }

    public String next() {
	String line;
	try {
	    line = reader.readLine();
	} catch (IOException e) {
	    line = null;
	}
	return line;
    }

    public int count(String name) {
	// System.out.println("Current line: " + line);

	while (!line.startsWith(name)) {
	    if (name.compareTo(line) < 0) return -1;
	    line = next();
	} 

        return Integer.parseInt(line.substring(line.lastIndexOf('\t') + 1));	
    }

  public static void main(String [] args) {
      RawReader r = new RawReader(args[0]);

      System.out.println("total " + r.count("\t"));
      System.out.println("l " + r.count("l"));
      System.out.println("laa " + r.count("laa"));
      System.out.println("labor " + r.count("labor"));
      System.out.println("lemans " + r.count("lemans"));
      System.out.println("new brunswick" + r.count("new brunswick"));
      System.out.println("new new york" + r.count("new new york"));
      System.out.println("new york city" + r.count("new york city"));

      r.reset(args[0]);

      System.out.println("total " + r.count("\t"));
      System.out.println("l " + r.count("l"));
      System.out.println("laa " + r.count("laa"));
      System.out.println("labor " + r.count("labor"));
      System.out.println("lemans " + r.count("lemans"));
      System.out.println("new brunswick" + r.count("new brunswick"));
      System.out.println("new new york" + r.count("new new york"));
      System.out.println("new york city" + r.count("new york city"));

      String line = r.next();
      while(line != null) {
	  line = r.next();
      }

  }

}