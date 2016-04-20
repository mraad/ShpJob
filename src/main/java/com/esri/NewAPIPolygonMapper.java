package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.io.PolygonFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by nrobison on 4/20/16.
 */
public class NewAPIPolygonMapper extends Mapper<LongWritable, PolygonFeatureWritable, NullWritable, Writable>
{
    private final static Text NAME = new Text("CNTRY_NAME");

    private final Text m_text = new Text();
    private final Envelope m_envelope = new Envelope();

    public void map(
            final LongWritable key,
            final PolygonFeatureWritable val,
            final Context context) throws IOException, InterruptedException
    {
        val.polygon.queryEnvelope(m_envelope);
        final Point center = m_envelope.getCenter();
        m_text.set(String.format("%.6f\t%.6f\t%s",
                center.getX(), center.getY(), val.attributes.get(NAME).toString()));
//        collector.collect(NullWritable.get(), m_text);
//        context.write(NullWritable.get(), m_text);
        context.write(NullWritable.get(), m_text);
    }
}
