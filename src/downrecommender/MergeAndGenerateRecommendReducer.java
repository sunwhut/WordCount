package downrecommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * �ϲ������Ѿ����ֵĹ��־���
 * @author ������
 */
public class MergeAndGenerateRecommendReducer extends Reducer<VarLongWritable, VectorWritable, VarLongWritable, RecommendedItemsWritable>{
  private static final Logger logger = LoggerFactory.getLogger(MergeAndGenerateRecommendReducer.class);
  private int recommendationsPerUser;
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    recommendationsPerUser = context.getConfiguration().getInt("recomandItems.recommendationsPerUser", 5);
  }
  @Override
  protected void reduce(VarLongWritable userID, Iterable<VectorWritable> cooVectorColumn,Context context)
      throws IOException, InterruptedException {
    //������ͺϲ�
    Vector recommdVector = null;
    for(VectorWritable vector : cooVectorColumn){
      recommdVector = recommdVector == null ? vector.get() : recommdVector.plus(vector.get());
    }
    //���Ƽ�������������Ϊÿ��UserID�ҳ�topM���Ƽ�ѡ��(Ĭ���ҳ�5��)���˶��а���item��Ӧ�ķ�����������
    //ע���£�PriorityQueue���е�ͷһ������С��Ԫ��,�������������������1��Ϊ��Ϊ��Ӹ������Ԫ��ʱʹ�õ���ʱ�ռ�
    Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(recommendationsPerUser+1, ByValueRecommendedItemComparator.getInstance());
    
    Iterator<Element> it = recommdVector.nonZeroes().iterator();
    while(it.hasNext()){
      Element e = it.next();
      int itemID = e.index();
      float preValue = (float) e.get();
      //����������С���Ƽ�����������������item�ͷ���
      if(topItems.size() < recommendationsPerUser){
        topItems.add(new GenericRecommendedItem(itemID, preValue));
      }
      //��ǰitem��Ӧ�ķ����ȶ����е�item����С�������򽫶���ͷԭʼ������СԪ�أ����������ҽ���ǰitem�������������
      else if(preValue > topItems.peek().getValue()){
        topItems.add(new GenericRecommendedItem(itemID, preValue));
        //����ͷԪ�أ���СԪ�أ�
        topItems.poll();
      }
    }
    
    //���µ������е�Ԫ�ص�˳��
    List<RecommendedItem> recommdations = new ArrayList<RecommendedItem>(topItems.size());
    recommdations.addAll(topItems);//������������Ԫ����Ӽ�������ļ���
    Collections.sort(recommdations,ByValueRecommendedItemComparator.getInstance());//����
    
    //����Ƽ�������Ϣ
    logger.info(userID+" "+ new RecommendedItemsWritable(recommdations));
    context.write(userID, new RecommendedItemsWritable(recommdations));
    
  }
}