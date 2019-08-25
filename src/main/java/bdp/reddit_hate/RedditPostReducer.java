package bdp.reddit_hate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RedditPostReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // this is a hashmap of "hateword" to some stats about it
        // doing this so I can further group our results by hateword instead of just by subreddit
        HashMap<String, StatData> stats = new HashMap<>();
        int subCount = 0;      // total counts for "this subreddit"
        int commentCount = 0;  // total counts for "this subreddit"

        for (Text val : values) {
            
            MapOutWritable mow = new MapOutWritable(val.toString());

            // if this is a hateful post
            if (mow.getHateWords().length > 0) {
               for (String hateWord : mow.getHateWords()) {
                   // get the stats for this hateword, or a new StatData object
                   StatData stat = stats.getOrDefault(hateWord, new StatData());

                   // add new statistics into the running averages
                   stat.add(mow.isSubmission(), mow.getScore(), mow.getGilded(), mow.getControversiality());

                   // put the stat back into the HashMap
                   stats.put(hateWord, stat);
               }
           }

           if (mow.isSubmission()) subCount++;
           else                    commentCount++;

        }

        // write our final answers (per hateword)
        // keep in mind that this reduce is only one of many
        // this reduce is scoped to a single subreddit
        // therefore, at the end of our job, we will have
        // written a row per subreddit per hateword
        for (Map.Entry<String, StatData> e : stats.entrySet()) {
            String out = String.join("|",
                    e.getKey(),
                    Integer.toString(subCount),
                    Integer.toString(commentCount),
                    Integer.toString(e.getValue().submissionCount),
                    Integer.toString(e.getValue().commentCount),
                    Float.toString(e.getValue().avgScore),
                    Float.toString(e.getValue().avgGilded),
                    Float.toString(e.getValue().avgControversiality)
            );
                
            context.write(key, new Text(out));
        }
    }

    /**
     *  This is a class to store data about a particular hate word within "this subreddit".
     */
    private class StatData {
        public float avgScore;
        public float avgGilded;
        public float avgControversiality;

        public int submissionCount;
        public int commentCount;
        public int count;  // total count (used for averages)

        private StatData() {
            avgScore = 0;
            avgGilded = 0;
            avgControversiality = 0;
            count = 0;
            submissionCount = 0;
            commentCount = 0;
        }


        private void add(boolean isSubmission, float score, float gilded, float controversiality) {
            if (isSubmission) submissionCount++;
            else              commentCount++;

            avgScore = newAvg(avgScore, score);
            avgGilded = newAvg(avgGilded, gilded);
            avgControversiality = newAvg(avgControversiality, controversiality);
            count++;
        }

        private float newAvg(float oldAvg, float newNum) {
            return ((oldAvg * count) + newNum) / (count + 1);
        }
    }

}