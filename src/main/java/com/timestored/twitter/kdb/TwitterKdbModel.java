package com.timestored.twitter.kdb;

import java.io.IOException;
import java.util.List;

import twitter4j.conf.Configuration;

public class TwitterKdbModel {
	
	private final String host;
	private final int port;
	
	private StatusStreamSaver statusStreamSaver;
	private SearchSaver searchSaver;
	private final Configuration configuration;
	
	public TwitterKdbModel(Configuration configuration, String host, int port) {
		
		this.configuration = configuration;
		this.host = host;
		this.port = port;
		
		try {
			searchSaver = new SearchSaver(configuration, host, port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**  
	 * @return number of tweets that have been received since stream was last turned on
	 */
	public int getStreamCount() {
		return statusStreamSaver == null ? 0 : statusStreamSaver.getCount();
	}

	/**  @return number of tweets that have been received for the tag searches */
	public int getSearchCount() { return searchSaver == null ? 0 : searchSaver.getTweetsSent(); }
	
	public void setStream(boolean on) {
		System.out.println("setStream -> " + on);
		if(on) {
			if(statusStreamSaver == null) {
		    	try {
		    		statusStreamSaver = new StatusStreamSaver(configuration, host, port);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			statusStreamSaver.stop();
			statusStreamSaver = null;
		}
	}

	public void setSearch(boolean on) {
		System.out.println("setSearch -> " + on);
		searchSaver.setSearch(on);
	}
	
	public boolean isStreamOn() { return statusStreamSaver != null; }
	public boolean isSearchOn() { return false; }
	public void setSearch(List<String> queries) { searchSaver.setSearch(queries); }
}
