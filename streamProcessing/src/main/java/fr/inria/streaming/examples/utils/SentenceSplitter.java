package fr.inria.streaming.examples.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class SentenceSplitter implements Serializable {

	private static final long serialVersionUID = -3128471897126263903L;

	public String[] splitSentence(String line) {
		if (line == null) {
			return null;
		}
		
		if (line.length()==0) {
			return null;
		}
		
		if (line.matches("^\\s+$")) {
			return null;
		}
		String[] words = line.split("\\s+");
		if (words.length == 0) {
			return null;
		}
		
		ArrayList<String> result = new ArrayList<String>();
		for (String w : words) {
			if (!w.equals("") && !w.matches("\\s+")) {
				result.add(w);
			}
		}
		
		return result.toArray(new String[result.size()]);
	}
}
