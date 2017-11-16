import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p></p>
 * Created by zhezhiyong@163.com on 2017/11/16.
 */
public class RunJob {

    public static void main(String[] args) {
        Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://192.168.97.57:8021");
		config.set("yarn.resourcemanager.hostname", "slave3");
        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);
            job.setJobName("wc");

            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path("/usr/input/"));

            Path outPath = new Path("/usr/output/wc");
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            FileOutputFormat.setOutputPath(job, outPath);

            boolean f = job.waitForCompletion(true);

            if (f) {
                System.out.println("job任务执行成功");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
