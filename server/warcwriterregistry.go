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
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"sync"
)

type warcWriterRegistry struct {
	settings    settings.Settings
	dbAdapter   database.DbAdapter
	warcWriters map[string]*warcWriter
	lock        sync.Mutex
}

func newWarcWriterRegistry(settings settings.Settings, db database.DbAdapter) *warcWriterRegistry {
	return &warcWriterRegistry{settings: settings, warcWriters: make(map[string]*warcWriter), dbAdapter: db}
}

func (w *warcWriterRegistry) GetWarcWriter(collectionConf *config.ConfigObject, recordMeta *contentwriter.WriteRequestMeta_RecordMeta) *warcWriter {
	w.lock.Lock()
	defer w.lock.Unlock()

	key := collectionConf.GetMeta().GetName() + "#" + recordMeta.GetSubCollection().String()
	if ww, ok := w.warcWriters[key]; ok {
		return ww
	}

	ww := newWarcWriter(w.settings, w.dbAdapter, collectionConf, recordMeta)
	w.warcWriters[key] = ww
	return ww
}

func (w *warcWriterRegistry) Shutdown() {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, ww := range w.warcWriters {
		ww.Shutdown()
	}
}
