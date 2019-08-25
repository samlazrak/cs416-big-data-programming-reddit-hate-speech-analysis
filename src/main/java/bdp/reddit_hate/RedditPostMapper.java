package bdp.reddit_hate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class RedditPostMapper extends Mapper<Object, Text, Text, Text> {
    private MapOutWritable out = new MapOutWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String json = value.toString();
        JSONObject jo;
        try {
            jo = new JSONObject(json);
        } catch (JSONException e) {
            System.err.println("Couldn't parse the json!");
            System.err.println(json);
            return; // can't do anything here if we can't parse the JSON
        }

        String body = jo.optString("body", "");
        String title = jo.optString("title", "");
        String selfText = jo.optString("selftext", "");
        final String DELETED = "[deleted]";
        final String REMOVED = "[removed]";

        // bit of a subjective call here
        // if any part of the Reddit post is deleted or removed, then it probably isn't representative of the subreddit
        // so exit
        if (body.equals(DELETED) || body.equals(REMOVED)) return;
        if (title.equals(DELETED) || title.equals(REMOVED)) return;
        if (selfText.equals(DELETED) || selfText.equals(REMOVED)) return;

        if (body.equals("")) {
            out.setSubmission(true); // this is a submission, since it doesn't have a body (should have title and selftext
        } else {
            out.setSubmission(false);
        }

        String subreddit = jo.optString("subreddit");
        // if the subreddit isn't found or null, this is weird, get out
        if (subreddit == JSONObject.NULL) return;
        else out.setSubreddit(subreddit);

        String[] hateWords = getHateWords(body + " " + title + " " + selfText);
        if (hateWords.length == 0) {
            out.setHateful(false);
            // if we have no hate words, just emit the subreddit name so we can count total comments/submissions
            context.write(new Text(subreddit), new Text(out.toString()));
            return;
        } else {
            out.setHateful(true);
        }

        out.setHateWords(hateWords);
        out.setScore(jo.optInt("score", 0));
        out.setGilded(jo.optInt("gilded", 0));
        out.setControversiality(jo.optInt("controversiality", 0));

        context.write(new Text(subreddit), new Text(out.toString()));
    }

    private String[] getHateWords(String text) {
        if (text.trim().equals("")) {
            return new String[]{};
        } else {
            return TokenizerStemmer.getHateWords(text).split(" ");
        }
    }

}
