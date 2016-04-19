package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.io.PolygonFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
final class PolygonFeatureMap
        extends MapReduceBase
        implements Mapper<LongWritable, PolygonFeatureWritable, NullWritable, Writable>
{
    private final static Text NAME = new Text("CNTRY_NAME");

    private final Text m_text = new Text();
    private final Envelope m_envelope = new Envelope();

    public void map(
            final LongWritable key,
            final PolygonFeatureWritable val,
            final OutputCollector<NullWritable, Writable> collector,
            final Reporter reporter) throws IOException
    {
        val.polygon.queryEnvelope(m_envelope);
        final Point center = m_envelope.getCenter();
        m_text.set(String.format("%.6f\t%.6f\t%s",
                center.getX(), center.getY(), val.attributes.get(NAME).toString()));
        collector.collect(NullWritable.get(), m_text);
    }
}
