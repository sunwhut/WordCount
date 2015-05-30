package myrecommender;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class SourceDataToUserVectorReducer extends
		Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable> {

	public void reduce(VarLongWritable userID, Iterable<VarLongWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for(VarLongWritable itemPref : values){
            userVector.set((int)itemPref.get(), 1.0f);//RandomAccessSparseVector.set(index,value),用户偏好类型为boolean类型，将偏好值默认都为1.0f
        }
        context.write(userID, new VectorWritable(userVector));
	}

}
