package no.nb.nna.veidemann.contentwriter.warc;

import org.jwat.warc.WarcFileNaming;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class VeidemannWarcFileNaming implements WarcFileNaming {

    /**
     * <code>DateFormat</code> to the following format 'yyyyMMddHHmmss'.
     */
    protected final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * Prefix component.
     */
    protected final String filePrefix;

    /**
     * Host name component.
     */
    protected final String hostName;

    /**
     * Extension component (including leading ".").
     */
    protected final String extension;

    protected final static AtomicInteger sequenceNumber = new AtomicInteger(0);

    /**
     * Construct file naming instance.
     *
     * @param filePrefix prefix or null, will default to "Veidemann"
     * @param hostName   host name or null, if you want to use default local host name
     */
    public VeidemannWarcFileNaming(String filePrefix, String hostName) {
        this.filePrefix = Objects.requireNonNullElse(filePrefix, "Veidemann");
        this.hostName = hostName;
        extension = ".warc";
    }

    @Override
    public boolean supportMultipleFiles() {
        return true;
    }

    @Override
    public String getFilename(int sequenceNr, boolean bCompressed) {
        String dateStr = dateFormat.format(new Date());

        String filename = filePrefix + "-" + dateStr
                + "-" + hostName.replace("-", "_")
                + "-" + String.format("%05d", sequenceNumber.getAndIncrement()) + extension;
        if (bCompressed) {
            filename += ".gz";
        }
        return filename;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public String getHostName() {
        return hostName;
    }
}
