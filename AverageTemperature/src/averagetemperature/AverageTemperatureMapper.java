
package averagetemperature;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author slave
 */
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
public class AverageTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
 
@Override
public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output , Reporter reporter) throws IOException {
 
String [] line=value.toString().split(",");
String datePart=line[1];
String temp=line[10];
 
if(StringUtils.isNumeric(temp))
   try{  output.collect(new Text(datePart), new IntWritable(Integer.parseInt(temp)));}
    catch(NumberFormatException e){ };
     
    }
     
    }