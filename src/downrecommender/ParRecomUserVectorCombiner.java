package downrecommender;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * �����㲿���û��Ƽ������Ľ�����кϲ�����userID��Ӧ�Ĺ��������ķ�ֵ�������(ע�⣺���ֻ�ǽ�һ��map��������кϲ����������Ҳ��ֻ���ֽ��)
 * @author ������
 */
public class ParRecomUserVectorCombiner extends Reducer<VarLongWritable, VectorWritable, VarLongWritable, VectorWritable>{
  private static final Logger logger = LoggerFactory.getLogger(ParRecomUserVectorCombiner.class);
  @Override
  protected void reduce(VarLongWritable userID, Iterable<VectorWritable> coocVectorColunms,Context context)
      throws IOException, InterruptedException {
      
    Vector vectorColunms = null;
    
    for(VectorWritable  coocVectorColunm : coocVectorColunms){
      vectorColunms = vectorColunms == null ? coocVectorColunm.get() : vectorColunms.plus(coocVectorColunm.get());
    }
    logger.info(userID +" " + new VectorWritable(vectorColunms));
    context.write(userID, new VectorWritable(vectorColunms));
  }
}