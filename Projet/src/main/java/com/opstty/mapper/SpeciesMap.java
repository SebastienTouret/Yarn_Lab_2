package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMap extends Mapper<Object, Text, Text, Writable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private NullWritable nw = NullWritable.get();
    private String header = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Ignoring the header
        if(value.toString().equals(header))
            return;

        // Selecting the "Espece" column (4th)
        String str = value.toString().split(";")[4];

        try {
            word.set(str);
            context.write(word, one);
        } catch (Exception e) {
            word.set(str);
            context.write(word, nw);
        }
    }
}