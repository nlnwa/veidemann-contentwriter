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
	"context"
	"fmt"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"testing"
	"time"
)

const bufSize = 1024 * 1024
const warcdir = "testdata"

type serverAndClient struct {
	lis        *bufconn.Listener
	dbMock     *r.Mock
	server     *GrpcServer
	clientConn *grpc.ClientConn
	client     contentwriter.ContentWriterClient
}

func newServerAndClient() serverAndClient {
	serverAndClient := serverAndClient{}

	dbMockConn := database.NewMockConnection()
	dbMockConn.GetMock().On(r.Table("config").Get("c1")).Return(map[string]interface{}{
		"id": "c1",
		"meta": map[string]interface{}{
			"name": "c1",
		},
		"collection": map[string]interface{}{
			"collectionDedupPolicy": "HOURLY",
			"fileRotationPolicy":    "MONTHLY",
		},
	}, nil)
	serverAndClient.dbMock = dbMockConn.GetMock()

	configCache := database.NewConfigCache(dbMockConn, time.Duration(1))
	serverAndClient.lis = bufconn.Listen(bufSize)
	server := New("", 0, settings.NewMock(warcdir, 1), configCache)
	server.grpcServer = grpc.NewServer()
	contentwriter.RegisterContentWriterServer(server.grpcServer, server.service)
	go func() {
		if err := server.grpcServer.Serve(serverAndClient.lis); err != nil {
			panic(fmt.Errorf("Server exited with error: %v", err))
		}
	}()
	serverAndClient.server = server

	// Set up client
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return serverAndClient.lis.Dial()
	}
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		panic(fmt.Errorf("Failed to dial bufnet: %v", err))
	}
	serverAndClient.clientConn = conn
	serverAndClient.client = contentwriter.NewContentWriterClient(conn)

	return serverAndClient
}

func (s serverAndClient) close() {
	s.clientConn.Close() //nolint
	s.server.Shutdown()
}

type writeRequests []*contentwriter.WriteRequest

var writeReq1 writeRequests = writeRequests{
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Meta{Meta: &contentwriter.WriteRequestMeta{
		ExecutionId: "eid1",
		TargetUri:   "http://www.example.com/foo.html",
		RecordMeta: map[int32]*contentwriter.WriteRequestMeta_RecordMeta{
			0: {
				RecordNum:         0,
				Type:              contentwriter.RecordType_REQUEST,
				Size:              263,
				RecordContentType: "application/http;msgtype=request",
				BlockDigest:       "sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932",
				PayloadDigest:     "sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709",
			},
			1: {
				RecordNum:         1,
				Type:              contentwriter.RecordType_RESPONSE,
				Size:              257,
				RecordContentType: "application/http;msgtype=response",
				BlockDigest:       "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4",
				PayloadDigest:     "sha1:C37FFB221569C553A2476C22C7DAD429F3492977",
			},
		},
		FetchTimeStamp: timestamppb.Now(),
		IpAddress:      "127.0.0.1",
		CollectionRef:  &config.ConfigRef{Kind: config.Kind_collection, Id: "c1"},
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 0,
		Data: []byte("GET / HTTP/1.0\n" +
			"Host: example.com\n" +
			"Accept-Language: en-US,en;q=0.8,ru;q=0.6\n" +
			"Referer: http://example.com/foo.html\n" +
			"Connection: close\n" +
			"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 1,
		Data: []byte(
			"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
				"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
				"Content-Length: 19\nConnection: close\nContent-Type: text/plain",
		),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Payload{Payload: &contentwriter.Data{
		RecordNum: 1,
		Data:      []byte("This is the content"),
	}}},
}

func TestContentWriterService_Write(t *testing.T) {
	err := os.Mkdir(warcdir, fileutil.PrivateDirMode)
	assert.NoError(t, err)

	now = func() time.Time {
		return time.Date(2000, 10, 10, 2, 59, 59, 0, time.UTC)
	}

	serverAndClient := newServerAndClient()
	serverAndClient.dbMock.
		On(r.Table("crawled_content").Get("sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c1_2000101002")).
		Return(nil, nil).Once()

	s := map[string]interface{}{
		"date":      r.MockAnything(),
		"digest":    "sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c1_2000101002",
		"targetUri": "http://www.example.com/foo.html",
		"warcId":    r.MockAnything(),
	}

	serverAndClient.dbMock.
		On(r.Table("crawled_content").Insert(s)).Return(&r.WriteResponse{Inserted: 1}, nil)

	ctx := context.Background()
	assert := assert.New(t)

	stream, err := serverAndClient.client.Write(ctx)
	assert.NoError(err)
	for i, r := range writeReq1 {
		err = stream.Send(r)
		assert.NoErrorf(err, "Error sending request #%d", i)
	}
	reply, err := stream.CloseAndRecv()
	assert.NoError(err)
	assert.Equal(2, len(reply.Meta.RecordMeta))

	fileNamePattern := `c1_2000101002-\d{14}-0001-\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.warc`

	assert.Equal(int32(0), reply.Meta.RecordMeta[0].RecordNum)
	assert.Equal(contentwriter.RecordType_REQUEST, reply.Meta.RecordMeta[0].Type)
	assert.Regexp("<urn:uuid:.{8}-.{4}-.{4}-.{4}-.{12}>", reply.Meta.RecordMeta[0].WarcId)
	assert.Equal("sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932", reply.Meta.RecordMeta[0].BlockDigest)
	assert.Equal("sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", reply.Meta.RecordMeta[0].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[0].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[0].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d$`, reply.Meta.RecordMeta[0].StorageRef)

	assert.Equal(int32(1), reply.Meta.RecordMeta[1].RecordNum)
	assert.Equal(contentwriter.RecordType_RESPONSE, reply.Meta.RecordMeta[1].Type)
	assert.Regexp("<urn:uuid:.{8}-.{4}-.{4}-.{4}-.{12}>", reply.Meta.RecordMeta[1].WarcId)
	assert.Equal("sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4", reply.Meta.RecordMeta[1].BlockDigest)
	assert.Equal("sha1:C37FFB221569C553A2476C22C7DAD429F3492977", reply.Meta.RecordMeta[1].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[1].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[1].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d\d$`, reply.Meta.RecordMeta[1].StorageRef)

	dirHasFilesMatching(t, warcdir, "^"+fileNamePattern+".open$", 1)
	serverAndClient.close()
	dirHasFilesMatching(t, warcdir, "^"+fileNamePattern+"$", 1)

	rmDir(warcdir)
}

func TestContentWriterService_WriteRevisit(t *testing.T) {
	err := os.Mkdir(warcdir, fileutil.PrivateDirMode)
	assert.NoError(t, err)

	now = func() time.Time {
		return time.Date(2000, 10, 10, 2, 59, 59, 0, time.UTC)
	}

	serverAndClient := newServerAndClient()
	serverAndClient.dbMock.
		On(r.Table("crawled_content").Get("sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c1_2000101002")).
		Return(map[string]interface{}{
			"date":      time.Date(2021, 8, 27, 13, 52, 0, 0, time.UTC),
			"digest":    "digest",
			"targetUri": "http://www.example.com",
			"warcId":    "<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>",
		}, nil).Once()

	ctx := context.Background()
	assert := assert.New(t)

	stream, err := serverAndClient.client.Write(ctx)
	assert.NoError(err)
	for i, r := range writeReq1 {
		err = stream.Send(r)
		assert.NoErrorf(err, "Error sending request #%d", i)
	}
	reply, err := stream.CloseAndRecv()
	assert.NoError(err)
	assert.Equal(2, len(reply.Meta.RecordMeta))

	fileNamePattern := `c1_2000101002-\d{14}-0001-\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.warc`

	assert.Equal(int32(0), reply.Meta.RecordMeta[0].RecordNum)
	assert.Equal(contentwriter.RecordType_REQUEST, reply.Meta.RecordMeta[0].Type)
	assert.Regexp("<urn:uuid:.{8}-.{4}-.{4}-.{4}-.{12}>", reply.Meta.RecordMeta[0].WarcId)
	assert.Equal("sha1:A3781FF1FC3FB52318F623E22C85D63D74C12932", reply.Meta.RecordMeta[0].BlockDigest)
	assert.Equal("sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", reply.Meta.RecordMeta[0].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[0].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[0].RevisitReferenceId)
	assert.Regexp(`warcfile:`+fileNamePattern+`:\d\d\d`, reply.Meta.RecordMeta[0].StorageRef)

	assert.Equal(int32(1), reply.Meta.RecordMeta[1].RecordNum)
	assert.Equal(contentwriter.RecordType_REVISIT, reply.Meta.RecordMeta[1].Type)
	assert.Regexp("<urn:uuid:.{8}-.{4}-.{4}-.{4}-.{12}>", reply.Meta.RecordMeta[1].WarcId)
	assert.Equal("sha1:BF9D96D3F3F230CE8E2C6A3E5E1D51A81016B55E", reply.Meta.RecordMeta[1].BlockDigest)
	assert.Equal("sha1:C37FFB221569C553A2476C22C7DAD429F3492977", reply.Meta.RecordMeta[1].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[1].CollectionFinalName)
	assert.Equal("<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>", reply.Meta.RecordMeta[1].RevisitReferenceId)
	assert.Regexp(`warcfile:`+fileNamePattern+`:\d\d\d\d`, reply.Meta.RecordMeta[1].StorageRef)

	dirHasFilesMatching(t, warcdir, "^"+fileNamePattern+".open$", 1)
	serverAndClient.close()
	dirHasFilesMatching(t, warcdir, "^"+fileNamePattern+"$", 1)

	rmDir(warcdir)
}

func dirHasFilesMatching(t *testing.T, dir string, pattern string, count int) bool {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	found := 0
	p := regexp.MustCompile(pattern)
	for _, file := range files {
		if p.MatchString(file.Name()) {
			found++
		}
	}
	if found != count {
		f := ""
		for _, ff := range files {
			f += "\n  " + ff.Name()
		}
		return assert.Fail(t, "Wrong number of files in '"+dir+"'", "Expected %d files to match %s, but found %d\nFiles in dir:%s", count, pattern, found, f)
	}
	return false
}

func rmDir(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}

	for _, file := range files {
		fileName := warcdir + "/" + file.Name()
		err = os.Remove(fileName)
		if err != nil {
			panic(err)
		}
	}
	err = os.Remove(dir)
	if err != nil {
		panic(err)
	}
}
