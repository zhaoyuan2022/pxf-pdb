package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;

public abstract class HcfsBaseAccessor extends BasePlugin implements Accessor {
    private static final CodecFactory codecFactory = CodecFactory.getInstance();

    /**
     * Checks if requests should be handled in a single thread or not.
     *
     * @return if the request can be run in multi-threaded mode.
     */
    @Override
    public boolean isThreadSafe() {
        return codecFactory.isCodecThreadSafe(context.getOption("COMPRESSION_CODEC"), context.getDataSource(), configuration);
    }
}
