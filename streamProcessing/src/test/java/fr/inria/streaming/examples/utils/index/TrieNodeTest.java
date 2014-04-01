package fr.inria.streaming.examples.utils.index;

import org.apache.log4j.Logger;
import org.junit.*;

import fr.inria.streaming.examples.utils.index.TrieNode.ImproperLetterException;
import static org.junit.Assert.*;

public class TrieNodeTest {

	private static Logger logger = Logger.getLogger(TrieNodeTest.class);
	
	@Test
	public void testCreation() throws ImproperLetterException {
		TrieNode node = new TrieNode();
		for (char c=0; c<=127; c++) {
			assertFalse(node.hasDescendant(c));
		}
	}
	
	@Test(expected=TrieNode.ImproperLetterException.class)
	public void shouldThrowExceptionIfCreatedWithImproperLetter() throws ImproperLetterException {
		TrieNode node = new TrieNode('.');
	}
	
	@Test
	public void testAddProperDescendants() throws ImproperLetterException {
		TrieNode node = new TrieNode();
		
		char a = 'a';
		try {
			node.addDescendantFor(a);
			assertTrue(node.hasDescendant(a));
			assertTrue(node.hasDescendant('A'));
		} catch (ImproperLetterException e) {
			logger.error(e.getMessage());
		}
		
		char Z = 'Z';
		try {
			node.addDescendantFor(Z);
			assertTrue(node.hasDescendant(Z));
			assertTrue(node.hasDescendant('z'));
		} catch (ImproperLetterException e) {
			logger.error(e.getMessage());
		}
	}
	
	@Test(expected=TrieNode.ImproperLetterException.class)
	public void shouldThrowExceptionForImproperChars() throws ImproperLetterException {
		TrieNode node = new TrieNode(' ');
		node.addDescendantFor(';');
	}
}
