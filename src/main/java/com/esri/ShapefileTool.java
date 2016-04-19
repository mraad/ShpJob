package com.esri;

import com.esri.mapred.PolygonFeatureInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 */
public class ShapefileTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new ShapefileTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;
        final JobConf jobConf = new JobConf(getConf(), ShapefileTool.class);

        if (args.length != 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            jobConf.setJobName(ShapefileTool.class.getSimpleName());

            jobConf.setMapperClass(PolygonFeatureMap.class);

            jobConf.setMapOutputKeyClass(NullWritable.class);
            jobConf.setMapOutputValueClass(Writable.class);

            jobConf.setNumReduceTasks(0);

            jobConf.setInputFormat(PolygonFeatureInputFormat.class);
            jobConf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
            final Path outputDir = new Path(args[1]);
            outputDir.getFileSystem(jobConf).delete(outputDir, true);
            FileOutputFormat.setOutputPath(jobConf, outputDir);

            JobClient.runJob(jobConf);
            rc = 0;
        }
        return rc;
    }
}
