package downrecommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * �����־���ͷָ����û��������кϲ����Ա���㲿�ֵ��Ƽ�����
 * @author ������
 */
public class CombineUserVectorAndCoocMatrixReducer extends Reducer<IntWritable, VectorOrPrefWritable, IntWritable, VectorAndPrefsWritable>{
  private static final Logger logger = LoggerFactory.getLogger(CombineUserVectorAndCoocMatrixReducer.class);
  @Override
  protected void reduce(IntWritable itemID, Iterable<VectorOrPrefWritable> values,Context context)
      throws IOException, InterruptedException {
    VectorAndPrefsWritable vectorAndPrefsWritable = new VectorAndPrefsWritable();
    List<Long> userIDs = new ArrayList<Long>();
    List<Float> preferenceValues = new ArrayList<Float>();
    Vector coocVector = null;
    Vector coocVectorTemp = null;
    Iterator<VectorOrPrefWritable> it = values.iterator();
    while(it.hasNext()){
      VectorOrPrefWritable e = it.next();
      coocVectorTemp = e.getVector() ;
      if(coocVectorTemp == null){
        userIDs.add(e.getUserID());
        preferenceValues.add(e.getValue());
      }else{
        coocVector = coocVectorTemp;
      }
    }
    if(coocVector != null){
      //�����Ҫע�⣬���ݹ��־���ļ���reduce�ۺ�֮�󣬵������һ��Reudce���������ֻ��һ��vecotr(�����־����һ�л���һ�У������к�����һ����)�ˡ�
      vectorAndPrefsWritable.set(coocVector, userIDs, preferenceValues);
      logger.info(itemID+" "+vectorAndPrefsWritable);
      context.write(itemID, vectorAndPrefsWritable);
    }
  }
}
