package bdp.reddit_hate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * MapReduce for munging information about Reddit hate speech (per subreddit, per hateword)
 * out of ~800GB of Reddit comment and submission data.
 *
 */
public class RedditHateSpeech {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if (args.length > 3 || args.length < 2) {
            System.err.println("Missing required arguments!");
            System.out.println("This program requires arguments in the following order:");
            System.out.println("<hdfs/comment/directory [hdfs/submission/directory] | hdfs/submission/directory> output/directory");
            System.exit(1);
            return;
        }

        Job job;
        Configuration conf = new Configuration();
        try {
            job = Job.getInstance(conf, "group1_reddit_hate_speech");
        } catch (IOException e) {
            System.err.println("IOException while trying to create job.");
            System.err.println("Exception message: " + e.getMessage());
            System.err.println("Exception cause: " + e.getCause());
            System.exit(1);
            return;
        }

        job.setJarByClass(RedditHateSpeech.class);
        job.setMapperClass(RedditPostMapper.class);
        job.setReducerClass(RedditPostReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (args.length == 3) {
            FileInputFormat.addInputPaths(job, args[0] + "," + args[1]);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        }
        else {
            FileInputFormat.addInputPaths(job, args[0]);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
