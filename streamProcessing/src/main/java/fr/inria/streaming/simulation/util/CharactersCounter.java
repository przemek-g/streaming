package fr.inria.streaming.simulation.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CharactersCounter implements Serializable {

	private static final long serialVersionUID = 7159302140499366484L;

	private static class CounterEntry {
		private char character;
		private int count;
		
		private CounterEntry(char c, int num) {
			this.character = c;
			this.count = num;
		}
		
		public char getCharacter() { return character; }
		public int getCount() { return count; }
	}
	
	private CounterEntry _maxEntry = null;
	private Map<Character,Integer> _charactersCount = new HashMap<Character,Integer>();
	
	public void count(char c) {
		if (!_charactersCount.containsKey(c)) {
			_charactersCount.put(c, 1);
			
			if (_maxEntry == null) {
				_maxEntry = new CounterEntry(c,1);
			}
		}
		else {
			_charactersCount.put(c, _charactersCount.get(c)+1);
			if (_charactersCount.get(c) > _maxEntry.getCount()) {
				_maxEntry = new CounterEntry(c,_charactersCount.get(c));
			}
		}
	}
	
	public char getMaxResultCharacter() {
		return _maxEntry.getCharacter();
	}
	
	public int getMaxResultCount() {
		return _maxEntry.getCount();
	}
}
