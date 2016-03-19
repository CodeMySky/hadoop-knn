import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class KnnReducer extends Reducer<Text, IntWritable, Text, Text> {
       private String[] no2Label = {"sitting", "sittingdown", "standing", "standingup", "walking"};
       public void reduce(Text testText, IntWritable labelNo, Context context) 
         throws IOException, InterruptedException {
           String label = no2Label[labelNo.get()];
           Text labelText = new Text();
           labelText.set(label);
           context.write(testText, labelText);
       }
    }
