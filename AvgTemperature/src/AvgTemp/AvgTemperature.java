/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AvgTemp;

/**
 *
 * @author slave
 */
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
 
public class AvgTemperature{
 
public static void main(String[] args) throws IOException{
 
 
    
JobConf conf= new JobConf(AvgTemperature.class);
conf.setJobName("Avg Temp");
 
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);
 
conf.setMapperClass(AvgTemperatureMapper.class);
conf.setReducerClass(AvgTemperatudeReducer.class);
 
FileInputFormat.addInputPath(conf, new Path(args[0]));
FileOutputFormat.setOutputPath(conf,new Path(args[1]));
 
FileSystem.getLocal(conf).delete(new Path(args[1]), true);
JobClient.runJob(conf);
 
}
 
}