package no.nb.nna.veidemann.contentwriter;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.WriteRequestMeta;

public class WriteSessionContextBuilder {

    private WriteRequestMeta.Builder writeRequestMeta;
    private ConfigObject collectionConfig;

    public WriteSessionContextBuilder withWriteRequestMeta(WriteRequestMeta.Builder writeRequestMeta) {
        this.writeRequestMeta = writeRequestMeta;
        return this;
    }

    public WriteSessionContextBuilder withCollectionConfig(ConfigObject collectionConfig) {
        this.collectionConfig = collectionConfig;
        return this;
    }

    public WriteSessionContext build() {
        WriteSessionContext context = new WriteSessionContext();

        context.collectionConfig = collectionConfig;
        context.writeRequestMeta = writeRequestMeta;

        return context;
    }
}
