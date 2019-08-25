package bdp.reddit_hate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MapOutWritable {

    private boolean isHateful;
    private boolean isSubmission; // true if submission, false if comment
    private String subreddit;
    private int score;
    private int gilded;
    private int controversiality;
    private String[] hateWords;

    public MapOutWritable() {
        // do nothin'
    }

    public MapOutWritable(String in) {
        String[] parts = in.split("\\|");
        int i = 0; // just cause I'm paranoid I'm gonna add more fields and mess up the indexing

        try {
            isHateful = Boolean.parseBoolean(parts[i++]);
            isSubmission = Boolean.parseBoolean(parts[i++]);
            subreddit = parts[i++];
            score = Integer.parseInt(parts[i++]);
            gilded = Integer.parseInt(parts[i++]);
            controversiality = Integer.parseInt(parts[i++]);
            hateWords = parts[i++].split(",");
        } catch (NumberFormatException e) {
            System.err.println("ERROR: NumberFormatException: " + e.getMessage());
            System.err.println("ERROR: Entire input string:");
            System.err.println("ERROR: " + in);
            e.printStackTrace();
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String hateWord : hateWords) {
            sb.append(hateWord).append(",");
        }

        return isHateful + "|" +
                isSubmission + "|" +
                subreddit + "|" +
                score + "|" +
                gilded + "|" +
                controversiality + "|" +
                sb.toString();
    }

    public boolean isHateful() {
        return isHateful;
    }

    public void setHateful(boolean hateful) {
        isHateful = hateful;
    }

    public boolean isSubmission() {
        return isSubmission;
    }

    public void setSubmission(boolean submission) {
        isSubmission = submission;
    }

    public String getSubreddit() {
        return subreddit;
    }

    public void setSubreddit(String subreddit) {
        this.subreddit = subreddit;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public int getGilded() {
        return gilded;
    }

    public void setGilded(int gilded) {
        this.gilded = gilded;
    }

    public int getControversiality() {
        return controversiality;
    }

    public void setControversiality(int controversiality) {
        this.controversiality = controversiality;
    }

    public String[] getHateWords() {
        return hateWords;
    }

    public void setHateWords(String[] hateWords) {
        this.hateWords = hateWords;
    }
}
