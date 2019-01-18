/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.contentwriter.warc;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.db.ProtoUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class WarcCollectionRegistry implements AutoCloseable {
    private final Map<String, WarcCollection> collections = new HashMap<>();

    public WarcCollection getWarcCollection(ConfigObject config) {
        WarcCollection c = collections.get(config.getId());
        if (c == null) {
            c = new WarcCollection(config);
            collections.put(config.getId(), c);
        } else if (c.shouldFlushFiles(config, ProtoUtils.getNowOdt())) {
            c.close();
            c = new WarcCollection(config);
            collections.put(config.getId(), c);
        }
        return c;
    }

    @Override
    public void close() {
        for (Iterator<Entry<String, WarcCollection>> it = collections.entrySet().iterator(); it.hasNext(); ) {
            it.next().getValue().close();
            it.remove();
        }
    }

    public void deleteFiles(ConfigObject config) throws IOException {
        close();
        WarcCollection c = new WarcCollection(config);
        c.deleteFiles();
        c.close();
    }
}
