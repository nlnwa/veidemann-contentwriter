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
	"time"
)

type ConfigCache interface {
	GetConfigObject(context.Context, *configV1.ConfigRef) (*configV1.ConfigObject, error)
	//GetScripts(context.Context, *configV1.BrowserConfig) ([]*configV1.ConfigObject, error)
}

type DbAdapter interface {
	GetConfigObject(context.Context, *configV1.ConfigRef) (*configV1.ConfigObject, error)
	//GetConfigsForSelector(context.Context, configV1.Kind, *configV1.Label) ([]*configV1.ConfigObject, error)
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

// getConfigsForSelector fetches configObjects by selector string (key:value)
//func (cc *configCache) getConfigsForSelector(ctx context.Context, selector string) ([]*configV1.ConfigObject, error) {
//	cached := cc.cache.GetMany(selector)
//	if cached != nil {
//		return cached, nil
//	}
//
//	t := strings.Split(selector, ":")
//	label := &configV1.Label{
//		Key:   t[0],
//		Value: t[1],
//	}
//
//	res, err := cc.db.GetConfigsForSelector(ctx, configV1.Kind_browserScript, label)
//	if err != nil {
//		return nil, err
//	}
//	cc.cache.SetMany(selector, res)
//
//	return res, nil
//}

//func (cc *configCache) GetScripts(ctx context.Context, browserConfig *configV1.BrowserConfig) ([]*configV1.ConfigObject, error) {
//	var scripts []*configV1.ConfigObject
//	for _, scriptRef := range browserConfig.ScriptRef {
//		script, err := cc.GetConfigObject(ctx, scriptRef)
//		if err != nil {
//			return nil, fmt.Errorf("failed to get script by reference %v: %w", scriptRef, err)
//		}
//		scripts = append(scripts, script)
//	}
//	for _, selector := range browserConfig.ScriptSelector {
//		configs, err := cc.getConfigsForSelector(ctx, selector)
//		if err != nil {
//			return nil, fmt.Errorf("failed to get scripts by selector %s: %w", selector, err)
//		}
//		for _, config := range configs {
//			scripts = append(scripts, config)
//		}
//	}
//	return scripts, nil
//}
