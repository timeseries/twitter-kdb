package com.timestored.twitter.kdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import kx.c;
import kx.c.Flip;
import kx.c.KException;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;

public class SearchSaver {
	
	// .qsocial.upd:{[tname; tdata] a:` sv `.qsocial,tname; $[tname in key `.qsocial; insert[a; tdata]; set[a;tdata]]}
	private static final int BUFFER_SIZE = 20;
	private static final int MAX_COUNT_ALLOWED = 100;

	
	private c conn;
	private List<String> keywords = new CopyOnWriteArrayList<String>();
	private Map<String, Long> querysMaxId = new HashMap<String, Long>();
	private final Twitter twitter;
	private boolean stopRequested; 
	private int tweetsSent = 0;
	
	private MyRunner searchRunner;
	private Thread searchRunnerThread;

	public SearchSaver(Configuration config, final String host, final int port) throws IOException {
        twitter = new TwitterFactory(config).getInstance();
        try {
			conn = new c(host, port);
		} catch (KException e) {
			throw new IOException(e);
		}

	}

	
	public void setSearch(List<String> queries) {
		keywords = new ArrayList<String>(queries);
		if(searchRunnerThread != null) {
			searchRunnerThread.interrupt();
		}
	}
	
    /**
     * Main entry of this application.
     *
     * @param args
     */
    public static void main(String[] args) throws KException, IOException {

    	SearchSaver ss = new SearchSaver(StatusStreamSaver.parseArgsToConfig(args), "localhost", 5001);
    	ss.setSearch(true);
    }

    private class MyRunner implements Runnable {
	    	
		@Override public void run() {
	        List<Status> tweets = new ArrayList<Status>();
	        
	        while(!stopRequested) {
	        	
		        for(String kw : keywords) {
			        System.out.println("Searching keywords: " + keywords.toString());
			        try {
			            Query query = new Query(kw);
			            query.setCount(MAX_COUNT_ALLOWED);
			            QueryResult result;
			            do {
			            	Long maxSeenId = querysMaxId.get(kw);
			            	if(maxSeenId != null) {
			            		query.setMaxId(maxSeenId);
			            	}
			                result = twitter.search(query);
			                
			                System.out.println("Found " + result.getCount() + " results for " + query.toString());
			            	
			                querysMaxId.put(kw, result.getMaxId());
			                tweets.addAll(result.getTweets());
			                
			                if(tweets.size()>BUFFER_SIZE) {
			                	Flip tab = KdbHelper.convertToTable(tweets);
			                	conn.ks(new Object[] { "insert".toCharArray(), "tweets", tab });
			                	tweetsSent += tweets.size();
			                	tweets.clear();
			                }
			            } while ((query = result.nextQuery()) != null);
			        } catch (TwitterException te) {
			            te.printStackTrace();
			            System.out.println("Failed to search tweets: " + te.getMessage());
			        } catch (IOException e) {
						e.printStackTrace();
					}
		
		        }
	        	
	        	try {
					Thread.sleep(1000*30);  // 30 Seconds
				} catch (InterruptedException e) {
					// handled by if condition
				}
	        }
	        System.out.println("stopping searches");
		}
    }
	
	public int getTweetsSent() {
		return tweetsSent;
	}

	public void setSearch(boolean on) {
		if(on) {
			if(searchRunner == null) {
	    		searchRunner = new MyRunner();
	    		searchRunnerThread = new Thread(searchRunner);
	    		searchRunnerThread.start();
			}
		} else {
			stopRequested = true;
			searchRunnerThread.interrupt();
			searchRunnerThread = null;
			searchRunner = null;
		}
	}
}
