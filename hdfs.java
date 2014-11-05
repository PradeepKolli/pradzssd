import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileReader {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream fin = fs.open(new Path(args[0]));
		BufferedReader br = new BufferedReader(new InputStreamReader(fin));
		Path outputFilePath = new Path(args[1]);
		FSDataOutputStream fout = null;
		try {
			String line;
			line = br.readLine();
			if (!fs.exists(outputFilePath)) {
				fout = fs.create(outputFilePath);
			} else {
				fout = fs.append(outputFilePath);
			}
			while (line != null) {

				String parsedLine = parseLine(line);
				
				fout.writeBytes(parsedLine+"\n");
				
				// be sure to read the next line otherwise you'll get an
				// infinite loop
				line = br.readLine();
			}
			fout.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	private static String parseLine(String line) {
		String[] tokens = line.split("\\|");
		if (tokens.length > 6) {
			return tokens[1] + "|" + tokens[3] + "|" + tokens[5];
		}
		return null;
	}
}
