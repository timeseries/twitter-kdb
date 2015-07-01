package com.timestored.twitter.trends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kx.c;
import kx.c.KException;
import twitter4j.Location;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Trend;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.timestored.twitter.kdb.KdbHelper;

public final class GetAvailableTrends {
	
	private c conn;
	
	public GetAvailableTrends(final String host, final int port) throws KException, IOException {
    	
		conn = new c(host, port);

		conn.ks(".qsocial.subs:`int$();");
		
		// called by the java API
		conn.ks(".qsocial.registerLocs:{ " +
				"a:`woeid xkey x; " +
				"$[`locations in key `.qsocial; upsert[`.qsocial.locations; a]; .qsocial.locations::a]};");
		
		conn.ks(".qsocial.addTrend:{ [woeid; asofDate; atDate; trendTab] " +
				"a:update woeid:woeid, asofDate:asofDate, atDate:atDate from trendTab;" +
				"$[`trends in key `.qsocial; insert[`.qsocial.trends; a]; .qsocial.trends::a]; };");
		
		conn.ks(".qsocial.addTweet:{ [tweetTab] " +
				"a:tweetTab;" +
				"$[`tweets in key `.qsocial; insert[`.qsocial.tweets; a]; .qsocial.tweets::a]; };");

		
		// called by the q API
		conn.ks(".qsocial.getLocations:{ .qsocial.locations };");
		conn.ks(".qsocial.getTrends:{ `woeid`asofDate`atDate xcols .qsocial.trends };");
		conn.ks(".qsocial.getTrends:{ .qsocial.tweets };");
		conn.ks(".qsocial.setLocationSubscription:{ .qsocial.subs:x; };");
		
		
        try {
        	ConfigurationBuilder cb = new ConfigurationBuilder();
//        	cb.setDebugEnabled(true)
//        	  .setOAuthConsumerKey()
//        	  .setOAuthConsumerSecret()
//        	  .setOAuthAccessToken()
//        	  .setOAuthAccessTokenSecret();
        	
            Twitter twitter = new TwitterFactory(cb.build()).getInstance();
            ResponseList<Location> locations;
            locations = twitter.getAvailableTrends();
            
            sendToKdb(conn, locations);
            
            Map<String, Integer> trendQueries = new HashMap<String, Integer>();
            
            /**
             * Get the trends for all selected
             */
            
            while(true) {
            	trendQueries.clear();
            	System.out.println("Waiting to cycle round subscription again");
            	trySleep(500);
            	
				List<Integer> woeids = getKdbWoeidSubs(conn);
				for (int woeid : woeids) {
					Trends placeTrends = twitter.getPlaceTrends(woeid);
					countTrendQueries(trendQueries, placeTrends);
		            sendToKdb(conn, placeTrends);
		            sleepIfNearLimit(placeTrends.getRateLimitStatus());
				}
				
				/** For all queries over alllocations perform searches */
	            for(String q : trendQueries.keySet()) {
	    			List<Status> tweets = new ArrayList<Status>();
	    	        QueryResult result = twitter.search(new Query(q));
		            sendToKdb(conn, result);
	    	        
	    	        sleepIfNearLimit(result.getRateLimitStatus());
	    	        
	    	        
	    	        tweets.addAll(result.getTweets());
	            }
            }
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get trends: " + te.getMessage());
            System.exit(-1);
        }
	}


	public void sleepIfNearLimit(RateLimitStatus rls) {
		if(rls.getRemaining() < 2) {
			System.err.println("sleeping as at limit -> " + rls.toString());
			trySleep(rls.getSecondsUntilReset()*1000);
		}
	}


	public void trySleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void countTrendQueries(Map<String, Integer> trendQueries,
			Trends trends) {
		for (Trend t : trends.getTrends()) {
			String word = t.getQuery();
			Integer count = trendQueries.get(word);
			if (count == null) {
				trendQueries.put(word, 1);
			} else {
				trendQueries.put(word, count + 1);
			}
		}
	}

	/**
	 * @return The list of currently subscribed woeid's from the kdb server.
	 */
	private static List<Integer> getKdbWoeidSubs(c conn) {
		List<Integer> r = Collections.emptyList();
		try {
			int[] woeids = (int[]) conn.k(".qsocial.subs");
			if(woeids.length > 0) {
				r = new ArrayList<Integer>(woeids.length);
				for(int w : woeids) {
					r.add(w);
				}
			}
		} catch (KException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return r;
	}

	/**
	 * Send the latest locations to the kdb server.
	 */
    public static void sendToKdb(c conn, ResponseList<Location> locations) {
    	
    	c.Flip tab = KdbHelper.convertToTable(locations);
		
		// create the command to insert the table of data into the named table.
		
		try {
			Object[] updStatement = new Object[] { ".qsocial.registerLocs", tab };
			conn.ks(updStatement); // send asynchronously
			System.out.println("Sent " + locations.size() + " locations to kdb server");
		} catch (IOException e) {
			System.err.println("error sending feed to server.");
		}
	}

	/**
	 * Send the latest trends for a given woeid to the kdb server.
	 */
	private static void sendToKdb(c conn, Trends trends) {

		Trend[] ts = trends.getTrends();
		
		// create the vectors for each column
		c.Flip trendTab = KdbHelper.convertToTable(ts);
		
		// create the command to insert the table of data into the named table.
		
		try {
			int woeid = trends.getLocation().getWoeid();
			Object[] updStatement = new Object[] { ".qsocial.addTrend", 
					woeid, trends.getAsOf(), trends.getTrendAt(), trendTab };
			
			conn.ks(updStatement); // send asynchronously
			System.out.println("Sent " + ts.length + " Trends to kdb server");
		} catch (IOException e) {
			System.err.println("error sending feed to server.");
		}
	}


	private static void sendToKdb(c conn, QueryResult queryResult) {


		List<Status> statii = queryResult.getTweets();
		c.Flip tweetTab = KdbHelper.convertToTable(statii);
		
		// create the command to insert the table of data into the named table.
		
		try {
			Object[] updStatement = new Object[] { ".qsocial.addTweet", tweetTab };
			
			conn.ks(updStatement); // send asynchronously
			System.out.println("Sent " + statii.size() + " tweets to kdb server");
		} catch (IOException e) {
			System.err.println("error sending feed to server.");
		}
	}



	
	/**
     * Usage: java twitter4j.examples.trends.GetAvailableTrends
     *
     * @param args message
     * @throws IOException 
     * @throws KException 
     */
    public static void main(String[] args) throws KException, IOException {
    	new GetAvailableTrends("localhost", 5001);
    }
    
}
