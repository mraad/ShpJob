package com.esri;

import com.esri.mapreduce.PolygonFeatureInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by nrobison on 4/20/16.
 */
public class NewAPIShapefileTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new NewAPIShapefileTool(), args));
    }

    public int run(final String[] args) throws Exception
    {
        Configuration hadoopConf = this.getConf();

        Job job = Job.getInstance(hadoopConf, NewAPIShapefileTool.class.getSimpleName());
        job.setJarByClass(NewAPIShapefileTool.class);
        job.setMapperClass(NewAPIPolygonMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(PolygonFeatureInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        final Path outputDir = new Path(args[1]);
        outputDir.getFileSystem(hadoopConf)
                 .delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
