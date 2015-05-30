package downrecommender;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;

/**
 * �����־���ͷָ����û��������кϲ����Ա���㲿�ֵ��Ƽ�����
 * ���mapper��ʵû��ʲô�߼������ܣ�ֻ�ǽ����ݰ��������ʽ���
 * ע�⣺�����mapper����Ϊ���־���ͷָ����û�������������еĹ�ͬ�����2��Ŀ¼
 * mapper���룺<itemID,Vecotor<userID,preferenceValuce>> or <itemID,Vecotor<coocItemID,coocTimes>>
 * mapper���:<itemID,Vecotor<userID,preferenceValuce>/Vecotor<coocItemID,coocTimes>>
 * @author ������
 */
public class CombineUserVectorAndCoocMatrixMapper extends Mapper<IntWritable, VectorOrPrefWritable, IntWritable, VectorOrPrefWritable>{
  @Override
  protected void map(IntWritable itemID, VectorOrPrefWritable value,Context context)
      throws IOException, InterruptedException {
    context.write(itemID, value);
  }

}