package recommendations;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 娴嬭瘯mahout org.apache.mahout.cf.taste.hadoop.item.RecommenderJob
 * environment锛�
 * mahout0.9
 * hadoop2.2
 * @author fansy
 *
 */
public class RecommenderJobTest{
	//RecommenderJob rec = null;
	Configuration conf =null;
	@Before
	public void setUp(){
	//	rec= new RecommenderJob();
		conf= HadoopUtil.getConf();
		System.out.println("Begin to test...");
	}
	
	@Test
	public void testMain() throws Exception{
		long start = System.currentTimeMillis();
		String[] args ={
		 "-i","hdfs://192.1.2.201:8020//input/test/in/sorce1.txt",  
         "-o","hdfs://192.1.2.201:8020/input/test/out",  
         "-n","3","-b","false","-s","SIMILARITY_EUCLIDEAN_DISTANCE",  
         "--maxPrefsPerUser","7","--minPrefsPerUser","2"}; 
		ToolRunner.run(conf, new RecommenderJob(), args);
		long end = System.currentTimeMillis();
		System.out.println(end-start+"ms");
	}
	
	@After
	public void cleanUp(){
		
	}
}
