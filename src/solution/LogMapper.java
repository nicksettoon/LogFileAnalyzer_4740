package solution;
// internal imports

// java stdlib imports
import java.util.GregorianCalendar;
import java.util.Calendar;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.IOException;

// hadoop imports
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Nicklaus Settoon
 * mapper that parses a single log file line via regular expressions and outputs a key,value pair.
 * the key is the ip address of the log line
 * the value is the time when the request was made
 */

public class LogMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String logLine = value.toString(); // get log input string
		String ip = "";
		String datetime = "";

		/* regex for pulling out IP address.
		* (1-3 digits followed by a .) 3 times in a row, then followed by another set of 1-3 digits
		*/
		String ipRegex = "^(?:\\d{1,3}[.]){3}\\d{1,3}";
		Pattern ipPattern = Pattern.compile(ipRegex); // feed pattern object the regex
		Matcher ipMatcher = ipPattern.matcher(logLine); // feed matcher the search string

		/* regex for pulling out date and time info
		* 2 digits followed by a /, all the way to a : followed by 2 final digits
		*/
		String datetimeRegex = "\\d{2}/.*:\\d{2}\\s-";
		Pattern datetimePattern = Pattern.compile(datetimeRegex); // feed pattern object the regex
		Matcher datetimeMatcher = datetimePattern.matcher(logLine); // feed matcher the search string

		// if ip was found
		if (ipMatcher.find())
			ip = ipMatcher.group();
		else // print error
			System.err.println("No IP address found in log line: " + key);
		
		// if date and time were found
		if (datetimeMatcher.find()) {
			datetime = datetimeMatcher.group();
			// re-process datetime to remove space at the end of the group
			datetimeRegex = "\\s-";
			datetimePattern = Pattern.compile(datetimeRegex);
			datetimeMatcher = datetimePattern.matcher(datetime);
			datetimeMatcher.replaceAll(""); //replace space with ""
		}
		else // print error
			System.err.println("No datetime info in log line: " + key);

		// write result to output
		context.write(new Text(ip), new Text(datetime));
	}
}
