package fr.inria.streaming.examples.utils;

import java.io.Serializable;

public class WordsRecognizer implements Serializable {

	private static final long serialVersionUID = -2970934189164824974L;

	public boolean isStringAWord(String strToCheck) {
		if (strToCheck == null)
			return false;
		
		if (strToCheck.equals(""))
			return false;
		
		return !strToCheck.matches(new String("^.*\\s+.*$"));
	}
}
