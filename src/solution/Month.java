/**
 * 
 */
package solution;

/**
 * @author training
 *
 */
public enum Month {
	Jan (0),
	Feb (1),
	Mar (2),
	Apr (3),
	May (4),
	Jun (5),
	Jul (6),
	Aug (7),
	Sep (8),
	Oct (9),
	Nov (10),
	Dec (11);
	
	private final int month;
	
	Month(int month){
		this.month = month;
	}
	
	public int getMonth() {
		return this.month;
	}

}
