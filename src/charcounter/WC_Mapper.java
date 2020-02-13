/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package charcounter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author Timothy.Wamalwa
 */
class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> oc, Reporter reporter) throws IOException {
        String line = value.toString();
        String tokenizer[]=line.split("");
        for(String SingleChar: tokenizer){
            Text charkey = new Text(SingleChar);
            IntWritable One = new IntWritable(1);
            oc.collect(charkey, One);
        }
    }
    
}
