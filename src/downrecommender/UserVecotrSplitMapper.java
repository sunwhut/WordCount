package downrecommender;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ���û������ָ�Ա����Ʒ�Ĺ����������кϲ�
 * mapper����:<userID,Vector<itemIDIndex,preferenceValuce>....>
 * reducer���:<itemID,Vecotor<userID,preferenceValuce>....> 
 * @author ������
 */
public class UserVecotrSplitMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, VectorOrPrefWritable>{
  private static final Logger logger = LoggerFactory.getLogger(UserVecotrSplitMapper.class);
  @Override
  protected void map(VarLongWritable userIDWritable, VectorWritable value,Context context)
      throws IOException, InterruptedException {
    IntWritable itemIDIndex = new IntWritable();
    long userID = userIDWritable.get();
    Vector userVector = value.get();
    Iterator<Element> it = userVector.nonZeroes().iterator();//ֻȡ�ǿ��û�����
    while(it.hasNext()){
      Element e = it.next();
      int itemID = e.index();
      itemIDIndex.set(itemID);
      float preferenceValuce = (float) e.get();
      logger.info(itemIDIndex +" "+new VectorOrPrefWritable(userID,preferenceValuce));
      context.write(itemIDIndex, new VectorOrPrefWritable(userID,preferenceValuce));
    }
    
  }
}