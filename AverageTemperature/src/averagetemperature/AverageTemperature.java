

package averagetemperature;
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
 
public class AverageTemperature{
 
public static void main(String[] args) throws IOException{
 
 
    
JobConf conf= new JobConf(AverageTemperature.class);
conf.setJobName("Avg Temp");
 
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);
 
conf.setMapperClass(AverageTemperatureMapper.class);
conf.setReducerClass(AverageTemperatureReducer.class);
 
FileInputFormat.addInputPath(conf, new Path(args[0]));
FileOutputFormat.setOutputPath(conf,new Path(args[1]));
 
FileSystem.getLocal(conf).delete(new Path(args[1]), true);
JobClient.runJob(conf);
 
}
 
}
