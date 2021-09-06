/*
 * Copyright 2021 National Library of Norway.
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

package server

import (
	"fmt"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"os"
)

func (ww *warcWriter) warcInfoGenerator(recordBuilder gowarc.WarcRecordBuilder) error {
	payload := &gowarc.WarcFields{}
	payload.Set("format", fmt.Sprintf("WARC File Format %d.%d", ww.settings.WarcVersion().Major(), ww.settings.WarcVersion().Minor()))
	payload.Set("collection", ww.collectionConfig.GetMeta().GetName())
	payload.Set("description", ww.collectionConfig.GetMeta().GetDescription())
	if ww.subCollection != config.Collection_UNDEFINED {
		payload.Set("subCollection", ww.subCollection.String())
	}
	payload.Set("isPartOf", ww.CollectionName())
	h, e := os.Hostname()
	if e != nil {
		return e
	}
	payload.Set("host", h)

	_, err := recordBuilder.WriteString(payload.String())
	return err
}
