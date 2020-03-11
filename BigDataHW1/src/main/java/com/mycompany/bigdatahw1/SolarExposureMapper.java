/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bigdatahw1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Ray
 */
public class SolarExposureMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private DoubleWritable num = new DoubleWritable();
    private Text yearInfo = new Text();   
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {  
      String[] data = value.toString().split(",");
      if(data.length==6){
        String year = data[2];
        double result = Double.parseDouble(data[5]);
        yearInfo.set(year);
        num.set(result);
        context.write(yearInfo,num); 
      }
    }
}
