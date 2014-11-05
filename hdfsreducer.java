package com.nrg.mr.regress.model;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import com.mapr.regress.ThreeSegmentRegressor;

public class Model5PReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private static final int DATASIZE = 365;
	
	private Text resultRecord = new Text();
	
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		
		
		String[] tu;
		double[] t = new double[DATASIZE];
		double[] u = new double[DATASIZE];

		Vector temp = new DenseVector(DATASIZE);
		Vector usage = new DenseVector(DATASIZE);
		
		double c1;
        double c2;
        double slope_left;
        double slope_right;
        double baseline;
        String resultParameters;
        int i = 0;

		for (Text point : values) {
			tu = point.toString().split(":");
			u[i] = Double.parseDouble(tu[0]);
			t[i] = Double.parseDouble(tu[1]);
			i++;
		}
		
		 temp.assign(t);
         usage.assign(u);

         //Call the three segment regression function
         ThreeSegmentRegressor BestModel = new ThreeSegmentRegressor(temp,usage);
         c1 = BestModel.cut1();
         c2 = BestModel.cut2();
         slope_left = BestModel.slope1();
         slope_right = BestModel.slope2();
         baseline = BestModel.baseLine();

         resultParameters = key.toString() + "|" + Double.toString(c1) + "|" + Double.toString(c2) + "|" + Double.toString(slope_left) + "|" + Double.toString(slope_right) + "|" + Double.toString(baseline);
         resultRecord.set(resultParameters);
         context.write(NullWritable.get(), resultRecord);
         
	}
}
