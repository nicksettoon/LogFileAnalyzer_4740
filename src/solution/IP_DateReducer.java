/**
 * 
 */
package solution;

// java stdlib imports
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.HashMap;

// hadoop imports
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
/**
 * @author Nicklaus Settoon
 *
 */

public class IP_DateReducer extends Reducer<Text, Text, Text, Text> {
			
	@Override
	public void reduce(Text _ip, Iterable<Text> _datetimes, Context context) throws IOException, InterruptedException {

		// get iterator to initialize trackers with first element in the Reducer's set
		Iterator<Text> datetimes = _datetimes.iterator();
		// latestText holds apache formatted string of latest date stamp
		Text latestText = datetimes.next(); 
		// latestCalendar holds latest java.util.Calendar object
		Calendar latestCalendar = DatetextToCalendar(latestText);
		
		while (datetimes.hasNext())
		{
			Text newText = datetimes.next(); // get next Text string
			Calendar newCalendar = DatetextToCalendar(newText); // convert it to java.util.Calendar
			if (newCalendar.after(latestCalendar))
			{// if new Calendar is more recent
				latestCalendar = newCalendar; // set latest calendar
				latestText = newText; // update text tracker as well
			}
		} 
		
		// after loop is done write result to output
		context.write(_ip, latestText);
	}
	
	private Calendar DatetextToCalendar(Text _datetext) {
		if(_datetext.toString() == "")
		{// if string given to use is empty from mapping phase, immediately return old calendar
			System.err.println(this + " reducer got empty date field to work with.");
			// assuming all log files being analyzed will have been created after 1970
			return new GregorianCalendar(0,0,0,0,0,0);
		}

		// otherwise: 
		// init variables
		int day, month, year, hour, min, sec = 0;
		// split the datetime string along any non-word characters
		String[] datetimeunits = _datetext.toString().split("[\\W]");
		
		day = Integer.parseInt(datetimeunits[0]); // first element is day
		// use custom month enum to convert apache logfile month tags into appropriate java.util.Calendar integers
		month = Month.valueOf(datetimeunits[1]).getMonth();
		year = Integer.parseInt(datetimeunits[2]); // third element is year
		hour = Integer.parseInt(datetimeunits[3]); // hour
		min = Integer.parseInt(datetimeunits[4]); // minute
		sec = Integer.parseInt(datetimeunits[5]); // second
		
		// return new calendar object made from datetimeunits
		return new GregorianCalendar(day, month, day, hour, min, sec);
	}
}
