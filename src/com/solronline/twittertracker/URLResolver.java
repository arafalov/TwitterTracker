package com.solronline.twittertracker;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class URLResolver {

    private static final int KEYWORD_ARGS_OFFSET = 3;

    public static void main(String[] args) throws IOException {

        File inputFile = new File(args[0]);
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        File outputFile = new File(inputFile.getParent(), args[1]);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        System.out.println("Write processed content to: " + outputFile);
        File errorFile = new File(inputFile.getParent(), args[2]);
        BufferedWriter errorWriter = new BufferedWriter(new FileWriter(errorFile));

        String[] keywords = new String[args.length-KEYWORD_ARGS_OFFSET];
        //The rest of args contain keywords, lower case them to
        for (int i=0; i<keywords.length; i++) {
            System.out.printf("Looking for keyword: '%s'\n", args[i]);
            keywords[i] = args[i+KEYWORD_ARGS_OFFSET].toLowerCase();
        }


        for (int i=0; i< 10; i++) {
            String line = reader.readLine();
            if (line == null) { break;} //we are done

            String[] elements = line.split("[\t]");
            String originalURL = elements[1]; //need to reprocess the file to have tab not space after date

            System.out.println();
            System.out.printf("Original URL      : '%s'\n", originalURL);
            int resCode = -1;
            URL trackedURL = new URL(originalURL);
            HttpURLConnection.setFollowRedirects(false);

            try {
                HttpURLConnection connection = null;
                while(true) {
                    connection = (HttpURLConnection) trackedURL.openConnection();
                    connection.setRequestProperty("Accept-Encoding", "gzip");
                    connection.connect();
                    resCode = connection.getResponseCode();
                    if (!(
                            resCode == HttpURLConnection.HTTP_SEE_OTHER ||
                                    resCode == HttpURLConnection.HTTP_MOVED_PERM ||
                                    resCode == HttpURLConnection.HTTP_MOVED_TEMP)) {

                        //we are done, whether error or ok
                        break;
                    }
                    String location = connection.getHeaderField("Location");
                    if (location.startsWith("/")) {
                        location = trackedURL.getProtocol() + "://" + trackedURL.getHost() + location;
                    }
                    trackedURL = new URL(location);
                    System.out.printf("    Redirected URL: '%s'\n", trackedURL);
                }

                String contentType = connection.getContentType();
                boolean isCompressed = "gzip".equals(connection.getContentEncoding());
                System.out.printf("    Connected URL : '%s' (Content-Type: '%s'; Compressed: %b)\n", connection.getURL(), contentType, isCompressed);

                boolean hasKeyword = false;

                //label to jump to:
                keywordMatch:
                if (contentType.contains("text/html")) {

                    try (
                            InputStream is = connection.getInputStream();
                            InputStream fullIS = isCompressed?(new GZIPInputStream(is)): is;
                            InputStreamReader inputStreamReader = new InputStreamReader(fullIS);
                            BufferedReader contentReader = new BufferedReader(inputStreamReader)) {
                        String contentLine;
                        while ((contentLine = contentReader.readLine()) != null) {

                            String contentLineLC = contentLine.toLowerCase();
                            for (String keyword : keywords) {
                                int keywordPos = contentLineLC.indexOf(keyword);
                                if (keywordPos == -1) {
                                    continue;
                                }

                                int keywordEndPos = keywordPos + keyword.length();
                                StringBuffer annotatedLine = new StringBuffer();
                                annotatedLine.append(contentLine.substring(0, keywordPos));
                                annotatedLine.append("[[[");
                                annotatedLine.append(contentLine.substring(keywordPos, keywordEndPos));
                                annotatedLine.append("]]]");
                                annotatedLine.append(contentLine.substring(keywordEndPos));
                                System.out.printf("    Found keyword '%s' in text '%s'\n", keyword, annotatedLine);

                                hasKeyword = true;
                                break keywordMatch;
                            }
                        }
                    }
                } //end of keyword match
                if (!hasKeyword) {
                    System.out.println("    KEYWORDS NOT FOUND");
                }
                StringBuffer writeLine = new StringBuffer();
                writeLine
                        .append(elements[0]) //date-time
                        .append('\t')
                        .append(elements[1]) // original URL
                        .append('\t')
                        .append(trackedURL.toExternalForm()) //final URL
                        .append('\t')
                        .append(hasKeyword?"KeywordMatch":"KeywordMissing")
                        .append('\t')
                        .append(elements[2]) // Twitter message ID
                        .append('\t')
                        .append("https://twitter.com/zzz/status/").append(elements[2]) // Twitter URL, zzz will be normalized
                        ;

                for (int eIdx=3; eIdx< elements.length; eIdx++) {
                    writeLine.append('\t').append(elements[eIdx]);
                }
                writer.append(writeLine.toString());
                writer.newLine();
            } catch (IOException e) {
                System.out.printf("    ERROR processing '%s' due to '%s'\n", trackedURL, e.getMessage());
                errorWriter.write(line);
                errorWriter.write('\t');
                errorWriter.write(e.getMessage());
                errorWriter.newLine();
            }

        }
        writer.close();
        errorWriter.close();
        reader.close();
    }
}
