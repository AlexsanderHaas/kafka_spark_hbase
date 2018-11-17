package main;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Cl_JavaRecord implements java.io.Serializable {
	
	/*//private String log;

	private String ts;

	private String uid;*/

	private String word;

	/*public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}

	public String getTs() {
		return ts;
	}

	public void setTs(String ts) {
		this.ts = ts;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}
*/
	public String getWord() {
		return word;
	}

	public void setWord(String word) {

		// Get Gson object
		
		/* * Gson gson = new GsonBuilder().setPrettyPrinting().create();
		 * 
		 * Cl_BroLog lo_log;*/
		 

		// lo_log = gson.fromJson(word, Cl_BroLog.class);

		// System.out.println(lo_log.getTs());

		// this.word = word;

		this.word = word;

	}

}
