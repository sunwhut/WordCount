package downrecommender;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;


/**
 * mapper�����ʽ��userID:itemID1 itemID2 itemID3....
 * mapper�����ʽ:<userID,itemID>
 * @author ������
 */
public class SourceDataToItemPrefsMapper extends Mapper<LongWritable, Text, VarLongWritable, VarLongWritable>{
  //private static final Logger logger = LoggerFactory.getLogger(SourceDataToItemPrefsMapper.class);
  private static final Pattern NUMBERS = Pattern.compile("(\\d+)");
  private String line = null;
  
  @Override
  protected void map(LongWritable key, Text value,Context context)
      throws IOException, InterruptedException {
     line = value.toString();
     if(line == null) return ;
    // logger.info("line:"+line);
     Matcher matcher = NUMBERS.matcher(line);
     matcher.find();//Ѱ�ҵ�һ�����飬��userID
     VarLongWritable userID = new VarLongWritable(Long.parseLong(matcher.group()));//�����������mahout�ж������з�װ��
     VarLongWritable itemID = new VarLongWritable();
     while(matcher.find()){
       itemID.set(Long.parseLong(matcher.group()));
    //	 logger.info(userID + " " + itemID);
       context.write(userID, itemID);
     }
  }
}