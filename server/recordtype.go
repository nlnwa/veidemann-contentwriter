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
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
)

func ToGowarcRecordType(recordType contentwriter.RecordType) gowarc.RecordType {
	switch recordType {
	case contentwriter.RecordType_WARCINFO:
		return gowarc.Warcinfo
	case contentwriter.RecordType_RESPONSE:
		return gowarc.Response
	case contentwriter.RecordType_RESOURCE:
		return gowarc.Resource
	case contentwriter.RecordType_REQUEST:
		return gowarc.Request
	case contentwriter.RecordType_METADATA:
		return gowarc.Metadata
	case contentwriter.RecordType_REVISIT:
		return gowarc.Revisit
	case contentwriter.RecordType_CONVERSION:
		return gowarc.Conversion
	case contentwriter.RecordType_CONTINUATION:
		return gowarc.Continuation
	default:
		return 0
	}
}

func FromGowarcRecordType(recordType gowarc.RecordType) contentwriter.RecordType {
	switch recordType {
	case gowarc.Warcinfo:
		return contentwriter.RecordType_WARCINFO
	case gowarc.Response:
		return contentwriter.RecordType_RESPONSE
	case gowarc.Resource:
		return contentwriter.RecordType_RESOURCE
	case gowarc.Request:
		return contentwriter.RecordType_REQUEST
	case gowarc.Metadata:
		return contentwriter.RecordType_METADATA
	case gowarc.Revisit:
		return contentwriter.RecordType_REVISIT
	case gowarc.Conversion:
		return contentwriter.RecordType_CONVERSION
	case gowarc.Continuation:
		return contentwriter.RecordType_CONTINUATION
	default:
		return 0
	}
}
