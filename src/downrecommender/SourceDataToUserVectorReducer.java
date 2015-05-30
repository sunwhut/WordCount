package downrecommender;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reducer���룺<userID,Iterable<itemID>>
 * reducer���:<userID,VecotrWriable<index=itemID,valuce=pres>....>
 * @author ������
 */
public class SourceDataToUserVectorReducer extends Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable>{
  private static final Logger logger = LoggerFactory.getLogger(SourceDataToUserVectorReducer.class);
  @Override
  protected void reduce(VarLongWritable userID, Iterable<VarLongWritable> itemPrefs,Context context)
      throws IOException, InterruptedException {
    /**
     *  DenseVector������ʵ�־���һ�����������飬�������������򶼽��д洢���ʺ����ڴ洢�ܼ�������
      RandomAccessSparseVector ���ڸ������� HashMap ʵ�ֵģ�key ������ (int) ���ͣ�value �Ǹ����� (double) ���ͣ���ֻ�洢�����в�Ϊ�յ�ֵ�����ṩ������ʡ�
      SequentialAccessVector ʵ��Ϊ���� (int) ���ͺ͸����� (double) ���͵Ĳ������飬��Ҳֻ�洢�����в�Ϊ�յ�ֵ����ֻ�ṩ˳����ʡ�
      �û����Ը����Լ��㷨������ѡ����ʵ�����ʵ���࣬����㷨��Ҫ�ܶ�������ʣ�Ӧ��ѡ�� DenseVector ���� RandomAccessSparseVector������󲿷ֶ���˳����ʣ�SequentialAccessVector ��Ч��Ӧ�ø��á�
      ������������ʵ�֣��������ǿ�����ν����е����ݽ�ģ��������������ǡ���ζ����ݽ��������������Ա���� Mahout �ĸ��ָ�Ч�ľ����㷨��
     */
    Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
    for(VarLongWritable itemPref : itemPrefs){
      userVector.set((int)itemPref.get(), 1.0f);//RandomAccessSparseVector.set(index,value),�û�ƫ������Ϊboolean���ͣ���ƫ��ֵĬ�϶�Ϊ1.0f
    }
    logger.info(userID+" "+new VectorWritable(userVector));
    context.write(userID, new VectorWritable(userVector));
  }
}
