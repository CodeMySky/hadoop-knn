import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class KnnMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static class DataPoint implements Comparable<DataPoint> {
        private int[] m_data =  new int[12];
        private int m_label;
        private int m_distance;
        private static final Map<String, Integer> label2No;

        static {
            label2No = new HashMap<>();
            label2No.put("sitting", 0);
            label2No.put("sittingdown", 1);
            label2No.put("standing", 2);
            label2No.put("standingup", 3);
            label2No.put("walking", 4);
        }

        public DataPoint(String text) {
            String[] values = text.split(" ");
            for (int i = 0; i < 12; i++) {
                m_data[i] = Integer.parseInt(values[i]);
            }
            m_label = label2No.get(values[12]);
        }

        public void updateDistance(DataPoint b) {
            int distance = 0;
            for (int i = 0; i < m_data.length - 1; ++i) {
                int delta = this.get(i) - b.get(i);
                distance += delta * delta;
            }
            m_distance = distance;
        }

        public int get(int index) {
            return m_data[index];
        }

        public int getDistance() {
            return m_distance;
        }

        public int getLabel() {
            return m_label;
        }
        public int compareTo(DataPoint b) {
            return b.getDistance() - m_distance;
        }
    }

    private List<DataPoint> m_trainingSet = new ArrayList<>();
    private int K = 16;
    private int knn(String testRecordText) {
        PriorityQueue<DataPoint> heap = new PriorityQueue<>();
        DataPoint test = new DataPoint(testRecordText);
        for (DataPoint train : m_trainingSet) {
            train.updateDistance(test);
            heap.add(train);
            if (heap.size() > K) {
                heap.poll();
            }
        }
        int[] histogram = new int[5];
        while (!heap.isEmpty()) {
            histogram[heap.poll().getLabel()]++;
        }
        int maxCount = -1;
        int maxLabel = -1;
        for (int i = 0; i < 5; i++) {
            if ( histogram[i] > maxCount) {
                maxCount = histogram[i];
                maxLabel = i;
            }
        }
        return maxLabel;
    }

    public void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new
                    Path("/user/bigdataanalytics/train.csv"))));
            String line;
            while ((line = reader.readLine()) != null) {
                m_trainingSet.add(new DataPoint(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text testText, Context context) throws IOException, InterruptedException {
        int label = knn(testText.toString());
        IntWritable labelNo = new IntWritable();
        labelNo.set(label);
        context.write(testText, labelNo);
    }
}
