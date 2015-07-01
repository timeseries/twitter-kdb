package com.timestored.twitter.kdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kx.c;
import kx.c.Flip;
import kx.c.KException;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Sample Stream Saver that stores whatever it receives to kdb.
 */
public final class StatusStreamSaver {

	private c conn;
	private final int BUFFER_SIZE = 20;
	private List<Status> statii = new ArrayList<Status>(BUFFER_SIZE);
	private TwitterStream twitterStream;
	public int tweetsSent = 0;
	
	public StatusStreamSaver(Configuration config, final String host, final int port) throws IOException {

        twitterStream = new TwitterStreamFactory(config).getInstance();
        try {
			conn = new c(host, port);
		} catch (KException e) {
			throw new IOException(e);
		}
		
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
            	statii.add(status);
            	if(statii.size() == BUFFER_SIZE) {
            		Flip tab = KdbHelper.convertToTable(statii);
                	try {
						conn.ks(new Object[] { "insert".toCharArray(), "tweets", tab});
						tweetsSent += statii.size();
					} catch (IOException e) {
						e.printStackTrace();
					}
                	statii.clear();
            	}
            }

			@Override public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }
            @Override public void onTrackLimitationNotice(int numberOfLimitedStatuses) { }
            @Override public void onScrubGeo(long userId, long upToStatusId) { }
            @Override public void onStallWarning(StallWarning warning) { }
            @Override public void onException(Exception ex) { ex.printStackTrace(); }
        };
        twitterStream.addListener(listener);

        twitterStream.sample();
	}

	public void stop() { twitterStream.cleanUp(); }
	public int getCount() { return tweetsSent; }

	/**
	 * Attempts to parse the args passed and return {@link Configuration}
	 * if that's not possible it prints some help and calls System.exit
	 */
	public static Configuration parseArgsToConfig(String... args) {
		if(args.length < 4) {
			System.out.println("Four arguments are required:\r\n"
					+ "java -jar jarname.jar ConsumerKey ConsumerSecret AccessToken AccessTokenSecret");
			System.exit(1);
		}
		
    	ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey(args[0])
    	  .setOAuthConsumerSecret(args[1])
    	  .setOAuthAccessToken(args[2])
    	  .setOAuthAccessTokenSecret(args[3]);
    	return cb.build();
	}

    /** Main entry of this application.  */
    public static void main(String[] args) throws KException, IOException {
    	new StatusStreamSaver(parseArgsToConfig(args), "localhost", 5001);
    }


}
