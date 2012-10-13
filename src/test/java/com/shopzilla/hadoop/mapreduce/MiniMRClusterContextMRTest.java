/**
 * Copyright 2012 Shopzilla.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  http://tech.shopzilla.com
 *
 */

package com.shopzilla.hadoop.mapreduce;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Jeremy Lucas
 * @since 6/8/12
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:mini-mr-cluster-context.xml")
public class MiniMRClusterContextMRTest {

    @Resource (name = "miniMrClusterContext")
    private MiniMRClusterContext miniMRClusterContext;

    @Resource (name = "hadoopConfig")
    private Configuration configuration;

    @Test
    public void testWordCount() throws Exception {
        Path input = new Path("/user/test/keywords_data");
        Path output = new Path("/user/test/word_count");

        Job job = new Job(configuration);

        job.setJobName("Word Count Test");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        assertTrue("All files from /data classpath directory should have been copied into HDFS", miniMRClusterContext.getFileSystem().exists(input));

        job.waitForCompletion(true);

        assertTrue("Output file should have been created", miniMRClusterContext.getFileSystem().exists(output));

        final LinkedList<String> expectedLines = new LinkedList<String>();
        expectedLines.add("goodbye\t1");
        expectedLines.add("hello\t1");
        expectedLines.add("world\t2");

        miniMRClusterContext.processData(output, new Function<String, Void>() {
            @Override
            public Void apply(String line) {
                assertEquals(expectedLines.pop(), line);
                return null;
            }
        });
        assertEquals(0, expectedLines.size());
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

}
