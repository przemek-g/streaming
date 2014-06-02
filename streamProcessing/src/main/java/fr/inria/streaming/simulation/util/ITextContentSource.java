package fr.inria.streaming.simulation.util;

import java.io.Serializable;

public interface ITextContentSource extends Serializable {
	
	char[] getTextContent();

}
