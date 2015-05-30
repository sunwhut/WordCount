package downrecommender;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * ��װ������ҵ���������Ƽ���ҵ
 * @author ������
 */
public class PackageRecomendJob extends Configured implements Tool{
  String[] dataSourceInputPath = {"/hadoop/dataSourceInput"};
  String[] uesrVectorOutput = {"/hadoop/uesrVectorOutput/"};
  String[] userVectorSpliltOutPut = {"/hadoop/userVectorSpliltOutPut"};
  String[] cooccurrenceMatrixOuptPath = {"/hadoop/CooccurrenceMatrixOuptPath"};
  String[] combineUserVectorAndCoocMatrixOutPutPath = {"/hadoop/combineUserVectorAndCoocMatrixOutPutPath"};
  String[] caclPartialRecomUserVectorOutPutPath = {"/hadoop/CaclPartialRecomUserVectorOutPutPath"};
  
  protected void setup(Configuration configuration)
      throws IOException, InterruptedException {
    FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), configuration);
    Path p1 = new Path(uesrVectorOutput[0]);
    Path p2 = new Path(userVectorSpliltOutPut[0]);
    Path p3 = new Path(cooccurrenceMatrixOuptPath[0]);
    Path p4 = new Path(combineUserVectorAndCoocMatrixOutPutPath[0]);
    Path p5 = new Path(caclPartialRecomUserVectorOutPutPath[0]);

    if (hdfs.exists(p1)) {
      hdfs.delete(p1, true);
    } 
    if (hdfs.exists(p2)) {
      hdfs.delete(p2, true);
    } 
    if (hdfs.exists(p3)) {
      hdfs.delete(p3, true);
    } 
    if (hdfs.exists(p4)) {
      hdfs.delete(p4, true);
    } 
    if (hdfs.exists(p5)) {
      hdfs.delete(p5, true);
    }
  }
  @Override
  public int run(String[] args) throws Exception {
      	Configuration conf=getConf(); //��������ļ�����
      	setup(conf);
      //	DistributedCache.addArchiveToClassPath(new Path("/user/hadoop/z.zeng/distruteItemCF/lib"), conf);
      //���ü����û�������ҵ
      Job wikipediaToItemPrefsJob = HadoopUtil.prepareJob(
            "WikipediaToItemPrefsJob",
      			dataSourceInputPath, 
      			uesrVectorOutput[0], 
      			TextInputFormat.class, 
      			SourceDataToItemPrefsMapper.class, 
      			VarLongWritable.class, 
      			VarLongWritable.class, 
      			SourceDataToUserVectorReducer.class, 
      			VarLongWritable.class, 
      			VectorWritable.class, 
      			SequenceFileOutputFormat.class, 
      			conf);
      //���ü��㹲��������ҵ
      Job userVectorToCooccurrenceJob = HadoopUtil.prepareJob(
            "UserVectorToCooccurrenceJob",
      			uesrVectorOutput, 
      			cooccurrenceMatrixOuptPath[0], 
      			SequenceFileInputFormat.class, 
      			UserVectorToCooccurrenceMapper.class, 
      			IntWritable.class, 
      			IntWritable.class, 
      			UserVectorToCoocurrenceReducer.class, 
      			IntWritable.class, 
      			VectorOrPrefWritable.class, 
      			SequenceFileOutputFormat.class, 
      			conf);
      //���÷ָ��û�������ҵ
      Job userVecotrSplitJob = HadoopUtil.prepareJob(
            "userVecotrSplitJob",
      			uesrVectorOutput, 
      			userVectorSpliltOutPut[0], 
      			SequenceFileInputFormat.class, 
      			UserVecotrSplitMapper.class, 
      			IntWritable.class, 
      			VectorOrPrefWritable.class, 
      			SequenceFileOutputFormat.class, 
      			conf);
      //�ϲ����������ͷָ�֮����û�������ҵ
      //�������Ҫ���ָ��û������͹���������������һ����Ϊ����
      String[] combineUserVectorAndCoocMatrixIutPutPath = {cooccurrenceMatrixOuptPath[0],userVectorSpliltOutPut[0]};
      Job combineUserVectorAndCoocMatrixJob = HadoopUtil.prepareJob(
            "combineUserVectorAndCoocMatrixJob",
            combineUserVectorAndCoocMatrixIutPutPath,
      			combineUserVectorAndCoocMatrixOutPutPath[0], 
      			SequenceFileInputFormat.class, 
      			CombineUserVectorAndCoocMatrixMapper.class, 
      			IntWritable.class, 
      			VectorOrPrefWritable.class, 
      			CombineUserVectorAndCoocMatrixReducer.class, 
      			IntWritable.class, 
      			VectorAndPrefsWritable.class, 
      			SequenceFileOutputFormat.class, 
      			conf);
      //�����û��Ƽ�����
      Job caclPartialRecomUserVectorJob= HadoopUtil.prepareJob(
            "caclPartialRecomUserVectorJob",
            combineUserVectorAndCoocMatrixOutPutPath,
            caclPartialRecomUserVectorOutPutPath[0], 
            SequenceFileInputFormat.class, 
      			CaclPartialRecomUserVectorMapper.class, 
      			VarLongWritable.class, 
      			VectorWritable.class, 
      			ParRecomUserVectorCombiner.class,//Ϊmap����combiner��������IO
      			MergeAndGenerateRecommendReducer.class, 
      			VarLongWritable.class, 
      			RecommendedItemsWritable.class, 
      			TextOutputFormat.class, 
      			conf);
      
      //��������job
      if(wikipediaToItemPrefsJob.waitForCompletion(true)){
        if(userVectorToCooccurrenceJob.waitForCompletion(true)){
          if(userVecotrSplitJob.waitForCompletion(true)){
            if(combineUserVectorAndCoocMatrixJob.waitForCompletion(true)){
               int rs = caclPartialRecomUserVectorJob.waitForCompletion(true) ? 1 :0;
              return rs;
            }else{
              throw new Exception("�ϲ����������ͷָ�֮����û�������ҵʧ�ܣ���");
            }
          }else{
            throw new Exception("�ָ��û�������ҵʧ�ܣ���");
          }
        }else{
          throw new Exception("���㹲��������ҵʧ�ܣ���");
        }
      }else{
        throw new Exception("�����û�������ҵʧ�ܣ���");
      }
  }
  public static void main(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {
    try {
      int returnCode =  ToolRunner.run(new PackageRecomendJob(),args);
      System.exit(returnCode);
    } catch (Exception e) {
    }
  }

}