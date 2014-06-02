package fr.inria.streaming.simulation.util;

import java.util.ArrayList;
import java.util.List;

public class FakeTweetContentSource implements ITextContentSource {

	private static List<char[]> tweets = new ArrayList<char[]>();
	
	static {
		tweets.add("If only Bradley's arm was longer. Best photo ever. #oscars pic.twitter.com/C9U5NOtGap".toCharArray());
		tweets.add("Thank you all for helping me through this time with your enormous love & support. Cory will forever be in my heart. pic.twitter.com/XVlZnh9vOc".toCharArray());
		tweets.add("Full picture of tonight's selfie wouldn't fit on insta pic.twitter.com/3W1e2EDdJY".toCharArray());
		tweets.add("You know what @onedirection ? Your fans REALLY love you, and it's beautiful to see! Enjoy your Moonman! Love, Seashell Girl *winks* xx".toCharArray());
		tweets.add("29 gold, 17 silver, 19 bronze - We finished 3rd in medal table after most successful Olympics for 104 years #OurGreatestTeam RT your support".toCharArray());
		tweets.add("WAKE UP EUROPE!!! The #PRISMATICWORLDTOUR is coming for u next FEB & March! See http://www.katyperry.com/events  for dates & how to get ur tix!".toCharArray());
		tweets.add("¡Nairo, tu fuerza viene del campo colombiano! ¡¡Gracias campeón por darnos el primer lugar en Il Giro d'Italia!! Shak".toCharArray());
		tweets.add("President Obama is acting on public health—with the first-ever national limits on carbon pollution from existing power plants. #ActOnClimate".toCharArray());
		tweets.add("899 : c'est le nombre de paquets de 5 cartes qu'il faut en moyenne pour remplir l'album Panini du Mondial 2014. Bon courage.".toCharArray());
		tweets.add("L'égalité entre les femmes et les hommes est au cœur de la République. Aujourd'hui, 8 mars, demain et tous les autres jours de l'année.".toCharArray());
	}
	
	@Override
	public char[] getTextContent() {
		return tweets.get((int)(Math.random()*10));
	}
	
	
}
