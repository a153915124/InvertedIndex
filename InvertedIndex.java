import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
        //Mapper：提取单词并将每个单词以"单词:文件名"->"1"的格式传出        
        public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
                private String pattern = "[^a-zA-Z0-9-]";
		private FileSplit split;
                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        //提取文件名
			split = (FileSplit)context.getInputSplit();
			int index = split.getPath().toString().indexOf("file");
			String fileName = split.getPath().getName();

                        String line = value.toString();
                        line = line.replaceAll(pattern, " ");
                        String[] words = line.split("\\s+");
                        for (String word : words) {
                                if (word.length() > 0) {
                                        //将key拼接为单词:文件名的格式，输出的value用text传出
                                        context.write(new Text(new Text(word)+":"+fileName), new Text("1"));
                                }
                        }
                }
        }
        //Combiner：累加Mapper传来的value值，并以"单词"->"文件名:词频"的格式传出
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
                
                protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        int count=0;
			for(Text value : values){
                                //要将text转为string再转为整数
				count+=Integer.parseInt(value.toString());
			}
			//提取出单词和文件名
			int index = key.toString().indexOf(":");
			String sKey = key.toString().substring(0, index);
			Text outKey = new Text(sKey);
			
			//将输出的key整合为单词，输出的value整合为 "（文件名，词频）"
			String sValue = key.toString().substring(index+1);
			Text outValue = new Text("("+sValue+", "+count+")");
                        context.write(outKey,outValue);
                }
        }
        //Reducer：拼接每个单词对应的value，格式为：(文件名1，词频), (文件名2，词频)……
        public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
                
                protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {                     
                        String fileList = new String();
			for(Text value:values){
				fileList+=value.toString()+", ";
			}
                        //去掉末尾的", "
			fileList=fileList.substring(0,fileList.length()-2);
			context.write(key, new Text(fileList));
                }
        }
        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "InvertedIndex");
                job.setJarByClass(InvertedIndex.class);
                job.setMapperClass(InvertedIndexMapper.class);
                //要设置一下Combiner类
		job.setCombinerClass(InvertedIndexCombiner.class);
                job.setReducerClass(InvertedIndexReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                boolean b = job.waitForCompletion(true);
                if(!b) {
                        System.out.println("InvertedIndex task fail!");
                }
        }
}
