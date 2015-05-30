package downrecommender;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ���㲿���û��Ƽ�����
 * @author ������
 */
public class CaclPartialRecomUserVectorMapper extends Mapper<IntWritable,VectorAndPrefsWritable, VarLongWritable, VectorWritable>{
  private static final Logger logger = LoggerFactory.getLogger(CaclPartialRecomUserVectorMapper.class);
  @Override
  protected void map(IntWritable itemID, VectorAndPrefsWritable values,Context context)
      throws IOException, InterruptedException {
    Vector coocVectorColumn = values.getVector();
    List<Long> userIDs = values.getUserIDs();
    List<Float> preferenceValues = values.getValues();
    for(int i = 0; i< userIDs.size(); i++){
      long userID = userIDs.get(i);
      float preferenceValue = preferenceValues.get(i);
      logger.info("userID:" + userID);
      logger.info("preferenceValue:"+preferenceValue);
      //�����־�����userID��Ӧ������ˣ���������û���Ӧ���Ƽ��б����
      Vector preferenceParScores = coocVectorColumn.times(preferenceValue);
      context.write(new VarLongWritable(userID), new VectorWritable(preferenceParScores));
    }
  }
}