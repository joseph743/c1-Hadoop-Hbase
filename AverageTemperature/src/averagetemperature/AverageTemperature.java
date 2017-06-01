

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
 
FileInputFormat.addInputPath(conf, new Path(args[0])); //args[0] est l'argument input (fichier text) dans la directoire /input
FileOutputFormat.setOutputPath(conf,new Path(args[1])); //args[1] est l'argument output path .les resultat seront stocker dans ce path
 
FileSystem.getLocal(conf).delete(new Path(args[1]), true);
JobClient.runJob(conf);
 
 
 //Dans le CLI executez la commande : hadoop jar <path de Java jar file "/AverageTemerature/dist/AvgTemperature.jar"> <path de hdfs> <nom du programme a execute "AvgTemperature">
}
 
}
