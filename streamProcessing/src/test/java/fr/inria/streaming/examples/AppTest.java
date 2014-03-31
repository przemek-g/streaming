package fr.inria.streaming.examples;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public void testGetDesiredFileName()
    {
    	String[] args = null;
    	String result = App.getDesiredFileName(args);
    	assertNull(result);
    	
    	args = new String[]{"some strange words here"};
    	result = App.getDesiredFileName(args);
    	assertNull(result);
    	
    	args = new String[]{"-f"};
    	result = App.getDesiredFileName(args);
    	assertNull(result);
    	
    	args = new String[]{"-f", "'\"Dziady\" by Adam Mickiewicz'"};
    	result = App.getDesiredFileName(args);
    	assertEquals("'\"Dziady\" by Adam Mickiewicz'", result);
    }
}
