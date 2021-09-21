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
	"reflect"
	"testing"
	"time"
)

var (
	v1 = &configV1.ConfigObject{Kind: configV1.Kind_crawlJob, Id: "1", Meta: &configV1.Meta{Name: "1"}}
	v2 = &configV1.ConfigObject{Kind: configV1.Kind_crawlJob, Id: "2", Meta: &configV1.Meta{Name: "2"}}
	v3 = &configV1.ConfigObject{Kind: configV1.Kind_crawlJob, Id: "3", Meta: &configV1.Meta{Name: "3"}}
)

type dbConnMock struct {
	*MockConnection
	i int
}

func (d *dbConnMock) GetConfigObject(_ context.Context, _ *configV1.ConfigRef) (*configV1.ConfigObject, error) {
	d.i++
	switch d.i {
	case 1:
		return v1, nil
	case 2:
		return v2, nil
	default:
		return v3, nil
	}
}

func TestConfigCacheGet(t *testing.T) {
	tests := []struct {
		name       string
		sleep      time.Duration
		wantFirst  *configV1.ConfigObject
		wantSecond *configV1.ConfigObject
		wantErr    bool
	}{
		{"same", 10 * time.Millisecond, v1, v1, false},
		{"evicted", 110 * time.Millisecond, v1, v2, false},
	}
	for _, tt := range tests {
		//i := 0
		t.Run(tt.name, func(t *testing.T) {
			cc := NewConfigCache(&dbConnMock{}, 100*time.Millisecond)
			ref := &configV1.ConfigRef{Kind: configV1.Kind_crawlJob, Id: "1"}

			gotFirst, err := cc.GetConfigObject(context.Background(), ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("1 GetConfigObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotFirst, tt.wantFirst) {
				t.Errorf("1 GetConfigObject() got = %v, want %v", gotFirst, tt.wantFirst)
			}

			time.Sleep(tt.sleep)

			gotSecond, err := cc.GetConfigObject(context.Background(), ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("2 GetConfigObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotSecond, tt.wantSecond) {
				t.Errorf("2 GetConfigObject() got = %v, want %v", gotSecond, tt.wantSecond)
			}
			if gotSecond != tt.wantSecond {
				t.Errorf("2 GetConfigObject() got = %v, want %v", gotSecond, tt.wantSecond)
			}
		})
	}
}
