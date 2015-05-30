package downrecommender;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * reducer����:<itemID,Iterable<itemIDs>>
 * reducer���:<mainItemID,Vector<coocItemID,coocTime(���ִ���)>....>
 * @author ������
 */
public class UserVectorToCoocurrenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, VectorOrPrefWritable>{
  private static final Logger logger = LoggerFactory.getLogger(UserVectorToCoocurrenceReducer.class);
  @Override
  protected void reduce(IntWritable mainItemID, Iterable<IntWritable> coocItemIDs,Context context)
      throws IOException, InterruptedException {
    Vector coocItemIDVectorRow = new RandomAccessSparseVector(Integer.MAX_VALUE,100);
    for(IntWritable coocItem : coocItemIDs){
      int itemCoocTime = coocItem.get();
      coocItemIDVectorRow.set(itemCoocTime,coocItemIDVectorRow.get(itemCoocTime)+1.0);//�����ִ����ۼ�
    }
    logger.info(mainItemID +" "+new VectorOrPrefWritable(coocItemIDVectorRow));
    context.write(mainItemID, new VectorOrPrefWritable(coocItemIDVectorRow));//��¼mainItemID���������ֹ�ϵ
  }
}