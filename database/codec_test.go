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
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/rethinkdb/rethinkdb-go.v6/encoding"
	"testing"
	"time"
)

func TestEncodeCrawledContent(t *testing.T) {
	ts := time.Date(2021, 8, 27, 13, 52, 0, 0, time.UTC)

	s := &contentwriter.CrawledContent{
		Digest:    "digest",
		WarcId:    "<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>",
		TargetUri: "http://www.example.com",
		Date:      timestamppb.New(ts),
	}

	d, err := encoding.Encode(s)
	assert.NoError(t, err)

	expected := map[string]interface{}{
		"date":      ts,
		"digest":    "digest",
		"targetUri": "http://www.example.com",
		"warcId":    "<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>",
	}
	assert.Equal(t, expected, d)
}

func TestDecodeCrawledContent(t *testing.T) {
	ts := time.Date(2021, 8, 27, 13, 52, 0, 0, time.UTC)

	s := map[string]interface{}{
		"date":      ts,
		"digest":    "digest",
		"targetUri": "http://www.example.com",
		"warcId":    "<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>",
	}

	var d contentwriter.CrawledContent
	err := encoding.Decode(&d, s)
	assert.NoError(t, err)

	expected := contentwriter.CrawledContent{
		Digest:    "digest",
		WarcId:    "<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>",
		TargetUri: "http://www.example.com",
		Date:      timestamppb.New(ts),
	}
	assert.Equal(t, expected.Date.AsTime(), d.Date.AsTime())
	assert.Equal(t, expected.Digest, d.Digest)
	assert.Equal(t, expected.WarcId, d.WarcId)
	assert.Equal(t, expected.TargetUri, d.TargetUri)
}
