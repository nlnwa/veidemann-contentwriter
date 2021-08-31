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

package database

import (
	"context"
	configV1 "github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"time"
)

type ConfigCache interface {
	GetConfigObject(context.Context, *configV1.ConfigRef) (*configV1.ConfigObject, error)
	HasCrawledContent(ctx context.Context, revisitKey string) (*contentwriter.CrawledContent, error)
	WriteCrawledContent(ctx context.Context, crawledContent *contentwriter.CrawledContent) error
}

type DbAdapter interface {
	GetConfigObject(context.Context, *configV1.ConfigRef) (*configV1.ConfigObject, error)
	HasCrawledContent(ctx context.Context, revisitKey string) (*contentwriter.CrawledContent, error)
	WriteCrawledContent(ctx context.Context, crawledContent *contentwriter.CrawledContent) error
}

type configCache struct {
	db    DbAdapter
	cache *cache
}

func NewConfigCache(db DbAdapter, ttl time.Duration) ConfigCache {
	return &configCache{
		db:    db,
		cache: newCache(ttl),
	}
}

func (cc *configCache) GetConfigObject(ctx context.Context, ref *configV1.ConfigRef) (*configV1.ConfigObject, error) {
	cached := cc.cache.Get(ref.Id)
	if cached != nil {
		return cached, nil
	}

	result, err := cc.db.GetConfigObject(ctx, ref)
	if err != nil {
		return nil, err
	}

	cc.cache.Set(result.Id, result)

	return result, nil
}
func (cc *configCache) HasCrawledContent(ctx context.Context, revisitKey string) (*contentwriter.CrawledContent, error) {
	return cc.db.HasCrawledContent(ctx, revisitKey)
}

func (cc *configCache) WriteCrawledContent(ctx context.Context, crawledContent *contentwriter.CrawledContent) error {
	return cc.db.WriteCrawledContent(ctx, crawledContent)
}
