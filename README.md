## InvertedIndex程序

### 算法思路

在WordCount程序的基础上，该程序在算法上新增了Combiner类，即总共分为以下三个部分：

- Mapper类：以input文件夹中的每个文件作为输入，首先提取出每行的单词，同时保存其所在的文件名，对每一个单词都输出以 $$word:filename\ \rightarrow \ ''1''$$ 格式的一个映射；
- Combiner类：以Mapper传来的映射为输入，用WordCount的方法累加每个key对应的value值，得到每个文件中每个单词的词频。由于最终需要得到一个$$ word\ \rightarrow \ (filename1,\ count),\ (filename2,\ count)\cdots$$格式的映射，即需要对每个单词进行汇总，因此对每个传来的映射，对key进行切片分离出word和filename，输出以$$word\ \rightarrow\ (filename,\ count)$$格式的映射；
- Reducer类：以Combiner传来的映射为输入，对相同key（即同一个单词）对应的value，将其用字符串fileList拼接起来，最后以其作为value输出；

流程图如下：

![image-20200318185830640](C:\Users\acer\AppData\Roaming\Typora\typora-user-images\image-20200318185830640.png)

### 关键部分代码

- Mapper类

  ```java
  public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
          //定义key文本和FileSplit
          private Text keyInfo = new Text();
          private FileSplit split;
          // 统计词频时，需要去掉标点符号等符号，此处定义表达式
          private String pattern = "[^a-zA-Z0-9-]";
          
          @Override
          protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              //简单起见，只获取文件名
  			split = (FileSplit)context.getInputSplit();
  			int index = split.getPath().toString().indexOf("file");
  			String fileName = split.getPath().toString().substring(index);
              // 将每一行转化为一个String
              String line = value.toString();
              // 将标点符号等字符用空格替换，这样仅剩单词
              line = line.replaceAll(pattern, " ");
              // 将String划分为一个个的单词
              String[] words = line.split("\\s+");
              // 将每一个单词初始化为词频为1，如果word相同，会传递给Reducer做进一步的操作
              for (String word : words) {
                  if (word.length() > 0) {
                      //用单词:文件名的格式作为key
                      keyInfo.set(new Text(word)+":"+fileName);
  				    context.write(new Text(keyInfo), new IntWritable(1));
                  }
              }
          }
  ```

- Combiner类

  ```java
  //Combiner:将map传入的 单词:文件名-> 1 统计为 单词-> (文件名,词频)
      public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, Text> {
          @Override
          protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
              // 初始化词频总数为0
              Integer count = 0;
              // 对key是一样的单词，执行词频汇总操作，也就是同样的单词:文件名，若是再出现则词频累加
              for (IntWritable value : values) {
                  count += value.get();
              }
  
              int index = key.toString().indexOf(":");
  			// 重新设置key值为单词
  			String word = key.toString().substring(0, index);
  			Text outKey = new Text(word);
  			
  			// 重新设置value值由文件名和词频组成
  			String fileName = key.toString().substring(index+1);
  			Text outValue = new Text("("+fileName+", "+count+")");
  			//输出
  			context.write(outKey, outValue);
          }
      }
  ```

- Reducer类

  ```java
  //Reducer:将Combiner传入的 单词-> 文件名:词频 统计为 单词 -> (文件名1, 词频), (文件名2, 词频)
      public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
          @Override
          protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
              String fileList = new String();
              // 对key是一样的单词，执行词频汇总操作，也就是同样的单词，若是再出现则使其value值拼接在一起
              for (Text file : values) {
                  fileList += file.toString() + ", ";
              }
              //除去末尾的", "
              fileList = fileList.substring(0,fileList.length()-2);
              // 最后输出汇总后的结果，注意输出时，每个单词只会输出一次，紧跟着该单词的词频
              context.write(key, new Text(fileList));
          }
      }
  ```

