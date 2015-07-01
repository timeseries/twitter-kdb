package com.timestored.twitter.kdb;

import java.util.Date;
import java.util.List;

import kx.c;
import twitter4j.HashtagEntity;
import twitter4j.Location;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Trend;

public class KdbHelper {


	private static final String[] TWEET_COL_NAMES 
		= new String[] { "id", "createdAt", "source", "retweets", "favourites", "text", "lang", "tags" };
	
	public static c.Flip convertToTable(List<Status> statii) {
		
		// create the vectors for each column
    	int n = statii.size();
		System.out.print("Received " + n + " Trends. \r\n");
		long[] id = new long[n];
		Date[] createdAt = new Date[n];
		String[] source = new String[n];
		long[] retweets = new long[n];
		long[] favourites = new long[n];
		char[][] text = new char[n][];
		String[] lang = new String[n];
		String[][] tags = new String[n][];
		
		// loop through filling the columns with data
		for(int i=0; i<statii.size(); i++) {
			Status s = statii.get(i);
			id[i] = s.getId();
			createdAt[i] = s.getCreatedAt();
			lang[i] = ss(s.getIsoLanguageCode());
			source[i] = ss(s.getSource());
			retweets[i] = s.getRetweetCount();
			favourites[i] = s.getFavoriteCount();
			text[i] = ss(s.getText()).toCharArray();
			lang[i] = ss(s.getIsoLanguageCode());
			
			HashtagEntity[] htags = s.getHashtagEntities();
			tags[i] = new String[htags.length];
			for(int j=0; j<htags.length; j++) {
				tags[i][j] = ss(htags[j].getText());
			}
		}
		
		// create the table itself from the separate columns
		Object[] trendData = new Object[] { id, createdAt, source, retweets, favourites, text, lang, tags };
		c.Flip tweetTab = new c.Flip(new c.Dict(TWEET_COL_NAMES, trendData));
		return tweetTab;
	}



	/** Safe String, if null return empty string */
	private static String ss(String s) {
		return s == null ? "" : s;
	}
	


	private static final String[] TREND_COL_NAMES 
			= new String[] { "name", "query", "url" };



	public static c.Flip convertToTable(Trend[] ts) {
		int n = ts.length;
		String[] name = new String[n];
		String[] query = new String[n];
		String[] url = new String[n];
		
		// loop through filling the columns with data
		for(int i=0; i<ts.length; i++) {
			Trend t = ts[i];
			name[i] = ss(t.getName());
			query[i] = ss(t.getQuery());
			url[i] = ss(t.getURL());
		}
		
		// create the table itself from the separate columns
		Object[] trendData = new Object[] { name, query, url };
		c.Flip trendTab = new c.Flip(new c.Dict(TREND_COL_NAMES, trendData));
		return trendTab;
	}	

	
	private static final String[] LOCATION_COL_NAMES 
			= new String[] { "woeid", "countryCode", "countryName", 
				"placeCode", "placeName", "name", "url" };
	



	public static c.Flip convertToTable(ResponseList<Location> locations) {
		int numRecords = locations.size();
		System.out.print("Received " + numRecords + " locations. \r\n");
		
		// create the vectors for each column
		int[] woeid = new int[numRecords];
		String[] countryCode = new String[numRecords];
		String[] countryName = new String[numRecords];
		int[] placeCode = new int[numRecords];
		String[] placeName = new String[numRecords];
		String[] name = new String[numRecords];
		String[] url = new String[numRecords];
		
		// loop through filling the columns with data
		for(int i=0; i<locations.size(); i++) {
			Location l = locations.get(i);
			woeid[i] = l.getWoeid();
			countryCode[i] = ss(l.getCountryCode());
			countryName[i] = ss(l.getCountryName());
			placeCode[i] = l.getPlaceCode();
			placeName[i] = ss(l.getPlaceName());
			name[i] = ss(l.getName());
			url[i] = ss(l.getURL());
		}
		
		// create the table itself from the separate columns
		Object[] data = new Object[] { woeid, countryCode, countryName, placeCode, placeName, name, url };
		c.Flip tab = new c.Flip(new c.Dict(LOCATION_COL_NAMES, data));
		return tab;
	}

}
