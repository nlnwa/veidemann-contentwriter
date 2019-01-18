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

import no.nb.nna.veidemann.api.config.v1.Collection.RotationPolicy;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.junit.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class WarcCollectionTest {

    @Test
    public void shouldFlushFiles() {
        ConfigObject.Builder config = ConfigObject.newBuilder();
        config.getCollectionBuilder().setFileRotationPolicy(RotationPolicy.NONE);

        WarcCollection col = new WarcCollection(config.build());
        OffsetDateTime now = ProtoUtils.getNowOdt();

        assertThat(col.shouldFlushFiles(config.build(), now)).isFalse();
        OffsetDateTime tomorrow = now.plusDays(1);
        assertThat(col.shouldFlushFiles(config.build(), tomorrow)).isFalse();

        config.getCollectionBuilder().setFileRotationPolicy(RotationPolicy.DAILY);
        assertThat(col.shouldFlushFiles(config.build(), now)).isTrue();
        assertThat(col.shouldFlushFiles(config.build(), tomorrow)).isTrue();

        col = new WarcCollection(config.build());
        assertThat(col.shouldFlushFiles(config.build(), now)).isFalse();
        assertThat(col.shouldFlushFiles(config.build(), tomorrow)).isTrue();
    }
}