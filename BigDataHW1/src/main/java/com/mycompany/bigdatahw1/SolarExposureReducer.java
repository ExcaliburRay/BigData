/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bigdatahw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Ray
 */
public class SolarExposureReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    private DoubleWritable minNum = new DoubleWritable();
    private DoubleWritable maxNum = new DoubleWritable();
    public void reduce(Text key, Iterable<DoubleWritable> values,Context context
                       ) throws IOException, InterruptedException {
        
        List<Double> list = new ArrayList<Double>();
        for(DoubleWritable val : values){
            list.add(val.get());
        }
        //bubble sort
        for(int i=0;i<list.size();i++){
            for(int j=0;j<list.size()-1;j++){
                if(list.get(j)>list.get(j+1)){
                    double temp = list.get(j);
                    list.set(j, list.get(j+1));
                    list.set(j+1,temp);
                }                   
            }
        }     
        double min = list.get(0);
        double max = list.get(list.size()-1);
        minNum.set(min);
        maxNum.set(max);
        context.write(key, minNum);
        context.write(key, maxNum);
    }
}
