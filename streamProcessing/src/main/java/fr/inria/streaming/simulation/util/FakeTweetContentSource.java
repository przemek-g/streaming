package fr.inria.streaming.simulation.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class FakeTweetContentSource implements ITextContentSource {

	private static final long serialVersionUID = -701439285752114025L;
	private static List<char[]> _originalTweets = new ArrayList<char[]>();
	private static char[][] _processedTweets;
	private static int _tweetLength = 0;;
	
	static {
		_originalTweets.add("If only Bradley's arm was longer. Best photo ever. #oscars pic.twitter.com/C9U5NOtGap".toCharArray());
		_originalTweets.add("Thank you all for helping me through this time with your enormous love & support. Cory will forever be in my heart. pic.twitter.com/XVlZnh9vOc".toCharArray());
		_originalTweets.add("Full picture of tonight's selfie wouldn't fit on insta pic.twitter.com/3W1e2EDdJY".toCharArray());
		_originalTweets.add("You know what @onedirection ? Your fans REALLY love you, and it's beautiful to see! Enjoy your Moonman! Love, Seashell Girl *winks* xx".toCharArray());
		_originalTweets.add("29 gold, 17 silver, 19 bronze - We finished 3rd in medal table after most successful Olympics for 104 years #OurGreatestTeam RT your support".toCharArray());
		_originalTweets.add("WAKE UP EUROPE!!! The #PRISMATICWORLDTOUR is coming for u next FEB & March! See http://www.katyperry.com/events  for dates & how to get ur tix!".toCharArray());
		_originalTweets.add("¡Nairo, tu fuerza viene del campo colombiano! ¡¡Gracias campeón por darnos el primer lugar en Il Giro d'Italia!! Shak".toCharArray());
		_originalTweets.add("President Obama is acting on public health—with the first-ever national limits on carbon pollution from existing power plants. #ActOnClimate".toCharArray());
		_originalTweets.add("899 : c'est le nombre de paquets de 5 cartes qu'il faut en moyenne pour remplir l'album Panini du Mondial 2014. Bon courage.".toCharArray());
		_originalTweets.add("L'égalité entre les femmes et les hommes est au cœur de la République. Aujourd'hui, 8 mars, demain et tous les autres jours de l'année.".toCharArray());
		
		_processedTweets = new char[_originalTweets.size()][];
		int maxLength = 0;
		for (char[] tweet : _originalTweets) {
			if (tweet.length > maxLength) {
				maxLength = tweet.length;
			}
		}
		// initialize the tweets with the same length as the longest original one
		setTweetLength(maxLength);
	}
	
	@Override
	public char[] getTextContent() {
//		return _originalTweets.get((int)(Math.random()*10));
		return _processedTweets[(int)(Math.random()*_originalTweets.size())];
	}
	
	public static void setTweetLength(int l) {
		if (l == _tweetLength || l < 1) { 
			return;
		}
		for (int i=0; i<_originalTweets.size(); i++) {
			if (_processedTweets[i] == null) {
				_processedTweets[i] = _originalTweets.get(i);
			}
			char[] currentProcessedTweet = _processedTweets[i];
			
			if (currentProcessedTweet.length < l) { // if the tweet needs to be longer than it is now
				_processedTweets[i] = StringUtils.rightPad(String.valueOf(currentProcessedTweet), l, ' ').toCharArray();
			}
			else if (currentProcessedTweet.length > l) { // if the tweet needs to be shorter than it is now
				_processedTweets[i] = String.valueOf(currentProcessedTweet,0,l).toCharArray();
			}
		}
		_tweetLength = l;
	}
	
	public static int getTweetLength() {
		return _tweetLength;
	}
}
