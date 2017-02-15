package com.solronline.twittertracker;

import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import twitter4j.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

    private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * If the search string is not provided, tweets are loaded from the raw tweets file
     * If the search string is provided, the search (before any exclusions) is stored to the raw tweets file
     * @param args - working directory, file to store raw tweets, search string
     */
    public static void main(String[] args) throws IOException {
        if (args.length > 2) {
            System.err.println("Usage: java -jar TwitterTracker.jar [workingdir] [searchQuery]");
            System.exit(-1);
        }

        Path workingPath = Paths.get((args.length>=1)?args[0]:".").toAbsolutePath().normalize();
        String searchQuery = (args.length==2)?args[1]:null;
        Path rawTweetsPath = workingPath.resolve("rawtweets.json");


        System.out.println("Tracking tweets using files in the directory: " + workingPath);


        long lastID;
        Path lastIDPath = workingPath.resolve("lastID.txt");
        lastID = getLastId(lastIDPath);
        if (lastID < 0) {
            System.out.println("No LastID found");
        } else {
            System.out.printf("LastID found: '%d'\n", lastID);
        }

        HashSet<String> excludedHandles = loadExclusions(workingPath.resolve("excluded-handles.txt"));
        System.out.printf("Found %d excluded handles\n", excludedHandles.size());

//        HashSet<String> excludedMentions = loadExclusions(workingPath.resolve("excluded-mentions.txt"));
//        System.out.printf("Found %d excluded mentions\n", excludedMentions.size());

        HashSet<String> excludedHosts = loadExclusions(workingPath.resolve("excluded-hosts.txt"));
        System.out.printf("Found %d excluded hosts\n", excludedHosts.size());

        Pattern includeTermsRegex = null;
        if (searchQuery != null) {
            // generate includeFilter from normal keywords in the search, as it does not seem to search text only
            String[] terms = searchQuery.split("[ ()\"]");
            StringBuilder regex = new StringBuilder();
            for (String term : terms) {
                if (term.indexOf(':')>=0) continue; //filter term
                if (term.length() == 0) continue; //empty
                if (term.equals("OR")) continue; //it was a query operator

                regex.append(term).append('|'); //we got this far
            }
            if (regex.length()>0){
                includeTermsRegex = Pattern.compile(regex.substring(0, regex.length()-1), Pattern.CASE_INSENSITIVE);
                System.out.println("Include regex: " + includeTermsRegex.toString());
            }
        }

        Pattern excludeTermsRegex = null;
        HashSet<String> filterExclude = loadExclusions(workingPath.resolve("excluded-terms.txt"));
        if (filterExclude.size() > 0) {
            String regex = filterExclude.stream().collect(Collectors.joining("|"));
            excludeTermsRegex = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            System.out.println("Exclude regex: " + excludeTermsRegex.toString());
        }


        List<Status> newTweets = null;
        if (searchQuery != null) {
             newTweets = getNewTweets(rawTweetsPath, searchQuery, lastID, 60);
        } else if (rawTweetsPath != null){
            newTweets = getStoredTweets(rawTweetsPath, lastID);
        } else {
            System.err.println("No query, no rawTweetsPath, nothing to do");
            System.exit(-2);
        }

        int newTweetsCount = newTweets.size();
        System.out.println("New tweets found: " + newTweetsCount);
        if (newTweetsCount == 0) {
            return; //we are done this iteration
        }

        lastID = newTweets.get(0).getId();

        BufferedWriter skippedTweetsWriter = Files.newBufferedWriter(workingPath.resolve("tweets-skipped.txt"), StandardOpenOption.APPEND,StandardOpenOption.CREATE);
        BufferedWriter acceptedTweetsWriter = Files.newBufferedWriter(workingPath.resolve("tweets-accepted.txt"), StandardOpenOption.APPEND,StandardOpenOption.CREATE);
        CloseableHttpClient httpclient = HttpClients.createDefault();


        processTweets:
        for (Status tweet : newTweets) {
            long tweetID = tweet.getId();

            //Skip tweet if it is a retweet
            if (tweet.isRetweet()) {
                writeSkipped(skippedTweetsWriter, tweetID, "retweet");
                continue processTweets;
            }

            //Skip tweet if it is from an excluded handle
            String screenName = tweet.getUser().getScreenName();
            if (excludedHandles.contains(screenName.toLowerCase())) {
                writeSkipped(skippedTweetsWriter, tweetID, "exclude due to handle '%s'", screenName);
                continue processTweets;
            }

            //Skip tweet if it mentions an excluded handle
            for (UserMentionEntity userMentionEntity : tweet.getUserMentionEntities()) {
                String mentionName = userMentionEntity.getScreenName();
                if (excludedHandles.contains(mentionName.toLowerCase())) {
                    writeSkipped(skippedTweetsWriter, tweetID, "exclude due to mention '%s'", mentionName);
                    continue processTweets;
                }
            }

            String tweetText = tweet.getText();
            //check the required and forbidden terms in the actual message text
            if (includeTermsRegex != null) {

                Matcher matcher = includeTermsRegex.matcher(tweetText);
                if (!matcher.find()) {
                    writeSkipped(skippedTweetsWriter, tweetID, "exclude due to missing required terms");
                    continue processTweets;
                }
            }

            if (excludeTermsRegex != null) {
                Matcher matcher = excludeTermsRegex.matcher(tweetText);
                if (matcher.find()) {
                    writeSkipped(skippedTweetsWriter, tweetID, "exclude due to forbidded term: '%s'", matcher.group());
                    continue processTweets;
                }
            }


            //deal with URLs
            System.out.printf("Accept tweet @%s: '%s'\n", tweet.getUser().getScreenName(), tweet.getText());
            URLEntity[] urlEntities = tweet.getURLEntities();
            for (URLEntity urlEntity : urlEntities) {
                HttpClientContext context = HttpClientContext.create();
                String initialURL = urlEntity.getExpandedURL();
                System.out.println("  url: " + initialURL.toString());
                HttpGet httpget = new HttpGet(initialURL);
                String initialHostName = URIUtils.extractHost(URI.create(initialURL)).getHostName();
                if (excludedHosts.contains(initialHostName)) { //don't even bother trying to resolve
                    writeSkipped(skippedTweetsWriter, tweetID, "exclude due to target host (initial) '%s'", initialHostName);
                    continue; //maybe another URL will work out, which will end up with same ID in both skipped and final URLs
                }

                try (CloseableHttpResponse response = httpclient.execute(httpget, context)) {

                    HttpHost target = context.getTargetHost();
                    List<URI> redirectLocations = context.getRedirectLocations();
                    URI location = URIUtils.resolve(httpget.getURI(), target, redirectLocations);
//                    if (redirectLocations != null) {
//                        for (URI redirectLocation : redirectLocations) {
//                            System.out.println("  redir: " + redirectLocation.toASCIIString());
//                        }
//                    }
                    System.out.println("  final: " + location.toASCIIString());
                    System.out.println();

                    String hostName = URIUtils.extractHost(location).getHostName();
                    if (excludedHosts.contains(hostName)) {
                        writeSkipped(skippedTweetsWriter, tweetID, "exclude due to target host '%s'", hostName);
                        continue; //maybe another URL will work out, which will end up with same ID in both skipped and final URLs
                    }

                    //rebuild URL to remove Google trackers (later do others too)
                    URIBuilder uriBuilder = new URIBuilder(location);
                    List<NameValuePair> queryParams = uriBuilder.getQueryParams();
                    uriBuilder.removeQuery();
                    for (NameValuePair queryParam : queryParams) {
                        if (!queryParam.getName().startsWith("utm_")){
                            uriBuilder.addParameter(queryParam.getName(), queryParam.getValue());
                        }
                    }
                    location = uriBuilder.build();
                    System.out.println("ACCEPTED URL: " + location);
                    System.out.println();
                    acceptedTweetsWriter.write(
                            String.format(
                                    "%s %s\t%d\t@%s\t%s\n",
                                    DATE_FORMAT.format(new Date()),
                                    location, tweetID, screenName, tweet.getText().replaceAll("\n", "    ")
                            ));
                } catch (URISyntaxException e) {
                    e.printStackTrace(System.err);
                } catch (ClientProtocolException e) {
                    e.printStackTrace(System.err);
                }

            }

        }

        skippedTweetsWriter.close();
        acceptedTweetsWriter.close();

        //Write out new lastID at the end
        try(BufferedWriter writer = Files.newBufferedWriter(lastIDPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)){
            writer.write(String.valueOf(lastID));
        }

        System.out.println("We are DONE!");
    }

    private static void writeSkipped(BufferedWriter skippedTweetsWriter, long tweetID, String format, String... params) throws IOException {
        skippedTweetsWriter.write(String.format("%s %d:", DATE_FORMAT.format(new Date()), tweetID));
        skippedTweetsWriter.write(String.format(format, (Object[])params)); //cast, so it knows it is params array
        skippedTweetsWriter.newLine();
    }

    private static List<Status> getStoredTweets(Path rawTweetsPath, long lastID) {
        LinkedList<Status> tweets = new LinkedList<>();
        if (Files.notExists(rawTweetsPath)) {
            System.err.println("Did not find raw tweets on the filesystem: " + rawTweetsPath.toString());
            return tweets; //empty
        }
        try {
            List<String> rawJSON = Files.readAllLines(rawTweetsPath);
            for (String rawTweet : rawJSON) {
                try {
                    Status tweet = TwitterObjectFactory.createStatus(rawTweet);
                    long tweetID = tweet.getId();
                    if (tweetID == lastID) {
                        System.out.println("Found lastID, no further tweets needed");
                        break;
                    }
                    tweets.add(tweet);
                } catch (TwitterException e) {
                    System.err.println("Was not able to parse JSON into a tweet. Skipping: " + rawTweet);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tweets;
    }

    private static List<Status> getNewTweets(Path rawTweetsPath, String searchQuery, long lastID, int maxTweets) {
        ArrayList<Status> newTweets = new ArrayList<>(maxTweets);

        System.out.printf("Retrieving live tweets with the query: '%s'\n", searchQuery);
        StringBuilder rawTweets = new StringBuilder();
        Twitter twitter = new TwitterFactory().getInstance();
        try{
            Query query = new Query(searchQuery);
            QueryResult result = null;

            getTweets:
            do {
                System.out.println("Running twitter search");
                result = twitter.search(query);
                List<Status> receivedTweets = result.getTweets();
                for (Status receivedTweet : receivedTweets) {
                    long currentTweetID = receivedTweet.getId();
//                        System.out.println("Current Tweet ID: " + currentTweetID);
                    if (currentTweetID == lastID) {
                        System.out.println("Found lastID, no further tweets needed");
                        break getTweets;
                    }
                    //write it out, add it to the list, check we are not done.
                    String rawJSON = TwitterObjectFactory.getRawJSON(receivedTweet);
                    rawTweets.append(rawJSON).append('\n');
                    newTweets.add(receivedTweet);
                    if (newTweets.size() == maxTweets) {
                        System.out.println("Retrieved maximum new tweets.");
                        break getTweets;
                    }
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException e) {
            e.printStackTrace();
        }

        try(FileWriter tweetWriter = new FileWriter(rawTweetsPath.toFile())){
            tweetWriter.write(rawTweets.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return newTweets;
    }

    private static HashSet<String> loadExclusions(Path exclusionPath) {
        HashSet<String> results = new HashSet<>();
        if (Files.notExists(exclusionPath)) {
            System.out.println("No exclusion file found at: " + exclusionPath.toAbsolutePath());
            return results;
        }

        try {
            List<String> lines = Files.readAllLines(exclusionPath);
            results.addAll(lines);
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }

        return results;
    }

    private static long getLastId(Path lastIDPath) {
        if (Files.notExists(lastIDPath)) {
            return -1;
        }

        try {
            String lastIDString = Files.readAllLines(lastIDPath).get(0);
            long lastID = Long.parseLong(lastIDString);
            return lastID;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
