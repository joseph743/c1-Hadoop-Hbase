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
import java.util.Iterator;
 
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 
 
 
public class AvgTemperatudeReducer extends MapReduceBase implements Reducer<Text,IntWritable, Text, IntWritable>{
 
public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output , Reporter reporter) throws IOException{
 
int sumTemps=0;
int numItems=0;
 
while(values.hasNext()){
sumTemps+=values.next().get();
numItems+=1;
}
output.collect(key, new IntWritable(sumTemps/numItems));
 
}
}