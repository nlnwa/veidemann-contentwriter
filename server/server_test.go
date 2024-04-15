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
	"io/ioutil"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

const bufSize = 1024 * 1024

type serverAndClient struct {
	lis        *bufconn.Listener
	dbMock     *r.Mock
	server     *GrpcServer
	clientConn *grpc.ClientConn
	client     contentwriter.ContentWriterClient
}

func newServerAndClient(settings settings.Settings) serverAndClient {
	serverAndClient := serverAndClient{}

	dbMockConn := database.NewMockConnection()
	dbMockConn.GetMock().
		On(r.Table("config").Get("c1")).Return(map[string]interface{}{
		"id": "c1",
		"meta": map[string]interface{}{
			"name": "c1",
		},
		"collection": map[string]interface{}{
			"collectionDedupPolicy": "HOURLY",
			"fileRotationPolicy":    "MONTHLY",
		}}, nil).
		On(r.Table("config").Get("c2")).Return(map[string]interface{}{
		"id": "c2",
		"meta": map[string]interface{}{
			"name": "c2",
		},
		"collection": map[string]interface{}{
			"collectionDedupPolicy": "HOURLY",
			"fileRotationPolicy":    "MONTHLY",
			"compress":              true,
		}}, nil)
	serverAndClient.dbMock = dbMockConn.GetMock()

	configCache := database.NewConfigCache(dbMockConn, time.Duration(1))
	serverAndClient.lis = bufconn.Listen(bufSize)
	server := New("", 0, settings, configCache)
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
	conn, err := grpc.NewClient(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

var writeReq1 = writeRequests{
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 0,
		Data: []byte("GET / HTTP/1.0\r\n" +
			"Host: example.com\r\n" +
			"Accept-Language: en-US,en;q=0.8,ru;q=0.6\r\n" +
			"Referer: http://example.com/foo.html\r\n" +
			"Connection: close\r\n" +
			"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\r\n",
		),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 1,
		Data: []byte("HTTP/1.1 200 OK\r\n" +
			"Date: Tue, 19 Sep 2016 17:18:40 GMT\r\n" +
			"Server: Apache/2.0.54 (Ubuntu)\r\n" +
			"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\r\n" +
			"ETag: \"3e45-67e-2ed02ec0\"\r\n" +
			"Accept-Ranges: bytes\r\n" +
			"Content-Length: 19\r\n" +
			"Connection: close\r\n" +
			"Content-Type: text/plain\r\n",
		),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Payload{Payload: &contentwriter.Data{
		RecordNum: 1,
		Data:      []byte("This is the content"),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Meta{Meta: &contentwriter.WriteRequestMeta{
		ExecutionId: "eid1",
		TargetUri:   "http://www.example.com/foo.html",
		RecordMeta: map[int32]*contentwriter.WriteRequestMeta_RecordMeta{
			0: {
				RecordNum:         0,
				Type:              contentwriter.RecordType_REQUEST,
				Size:              268,
				RecordContentType: "application/http;msgtype=request",
				BlockDigest:       "sha1:AD6944346BF47CEACBE14E387EB031FCBDB59227",
				PayloadDigest:     "sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709",
			},
			1: {
				RecordNum:         1,
				Type:              contentwriter.RecordType_RESPONSE,
				Size:              265,
				RecordContentType: "application/http;msgtype=response",
				BlockDigest:       "sha1:9D3D1E0589A62F6091F53482A843399B66926655",
				PayloadDigest:     "sha1:C37FFB221569C553A2476C22C7DAD429F3492977",
			},
		},
		FetchTimeStamp: timestamppb.Now(),
		IpAddress:      "127.0.0.1",
		CollectionRef:  &config.ConfigRef{Kind: config.Kind_collection, Id: "c1"},
	}}},
}

var writeReq2 = writeRequests{
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 0,
		Data: []byte("GET / HTTP/1.0\r\n" +
			"Host: example.com\r\n" +
			"Accept-Language: en-US,en;q=0.8,ru;q=0.6\r\n" +
			"Referer: http://example.com/foo.html\r\n" +
			"Connection: close\r\n" +
			"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36\r\n",
		),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
		RecordNum: 1,
		Data: []byte("HTTP/1.1 200 OK\r\n" +
			"Date: Tue, 19 Sep 2016 17:18:40 GMT\r\n" +
			"Server: Apache/2.0.54 (Ubuntu)\r\n" +
			"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\r\n" +
			"ETag: \"3e45-67e-2ed02ec0\"\r\n" +
			"Accept-Ranges: bytes\r\n" +
			"Content-Length: 19\r\n" +
			"Connection: close\r\n" +
			"Content-Type: text/plain\r\n",
		),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Payload{Payload: &contentwriter.Data{
		RecordNum: 1,
		Data:      []byte("This is the content"),
	}}},
	&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Meta{Meta: &contentwriter.WriteRequestMeta{
		ExecutionId: "eid1",
		TargetUri:   "http://www.example.com/foo.html",
		RecordMeta: map[int32]*contentwriter.WriteRequestMeta_RecordMeta{
			0: {
				RecordNum:         0,
				Type:              contentwriter.RecordType_REQUEST,
				Size:              268,
				RecordContentType: "application/http;msgtype=request",
				BlockDigest:       "sha1:AD6944346BF47CEACBE14E387EB031FCBDB59227",
				PayloadDigest:     "sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709",
			},
			1: {
				RecordNum:         1,
				Type:              contentwriter.RecordType_RESPONSE,
				Size:              265,
				RecordContentType: "application/http;msgtype=response",
				BlockDigest:       "sha1:9D3D1E0589A62F6091F53482A843399B66926655",
				PayloadDigest:     "sha1:C37FFB221569C553A2476C22C7DAD429F3492977",
			},
		},
		FetchTimeStamp: timestamppb.Now(),
		IpAddress:      "127.0.0.1",
		CollectionRef:  &config.ConfigRef{Kind: config.Kind_collection, Id: "c2"},
	}}},
}

func TestContentWriterService_Write(t *testing.T) {
	testSettings := settings.NewMock(t.TempDir(), 1)

	now = func() time.Time {
		return time.Date(2000, 10, 10, 2, 59, 59, 0, time.UTC)
	}

	serverAndClient := newServerAndClient(testSettings)
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

	fileNamePattern := `c1_2000101002-\d{14}-0001-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|.+).warc`

	assert.Equal(int32(0), reply.Meta.RecordMeta[0].RecordNum)
	assert.Equal(contentwriter.RecordType_REQUEST, reply.Meta.RecordMeta[0].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[0].WarcId)
	assert.Equal("sha1:AD6944346BF47CEACBE14E387EB031FCBDB59227", reply.Meta.RecordMeta[0].BlockDigest)
	assert.Equal("sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", reply.Meta.RecordMeta[0].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[0].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[0].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d$`, reply.Meta.RecordMeta[0].StorageRef)

	assert.Equal(int32(1), reply.Meta.RecordMeta[1].RecordNum)
	assert.Equal(contentwriter.RecordType_RESPONSE, reply.Meta.RecordMeta[1].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[1].WarcId)
	assert.Equal("sha1:9D3D1E0589A62F6091F53482A843399B66926655", reply.Meta.RecordMeta[1].BlockDigest)
	assert.Equal("sha1:C37FFB221569C553A2476C22C7DAD429F3492977", reply.Meta.RecordMeta[1].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[1].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[1].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d\d$`, reply.Meta.RecordMeta[1].StorageRef)

	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+".open$", 1)
	serverAndClient.close()
	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+"$", 1)
}

func TestContentWriterService_Write_Compressed(t *testing.T) {
	testSettings := settings.NewMock(t.TempDir(), 1)

	now = func() time.Time {
		return time.Date(2000, 10, 10, 2, 59, 59, 0, time.UTC)
	}

	serverAndClient := newServerAndClient(testSettings)
	serverAndClient.dbMock.
		On(r.Table("crawled_content").Get("sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c2_2000101002")).
		Return(nil, nil).Once()

	s := map[string]interface{}{
		"date":      r.MockAnything(),
		"digest":    "sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c2_2000101002",
		"targetUri": "http://www.example.com/foo.html",
		"warcId":    r.MockAnything(),
	}

	serverAndClient.dbMock.
		On(r.Table("crawled_content").Insert(s)).Return(&r.WriteResponse{Inserted: 1}, nil)

	ctx := context.Background()
	assert := assert.New(t)

	stream, err := serverAndClient.client.Write(ctx)
	assert.NoError(err)
	for i, r := range writeReq2 {
		err = stream.Send(r)
		assert.NoErrorf(err, "Error sending request #%d", i)
	}
	reply, err := stream.CloseAndRecv()
	assert.NoError(err)
	assert.Equal(2, len(reply.Meta.RecordMeta))

	fileNamePattern := `c2_2000101002-\d{14}-0001-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|.+).warc.gz`

	assert.Equal(int32(0), reply.Meta.RecordMeta[0].RecordNum)
	assert.Equal(contentwriter.RecordType_REQUEST, reply.Meta.RecordMeta[0].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[0].WarcId)
	assert.Equal("sha1:AD6944346BF47CEACBE14E387EB031FCBDB59227", reply.Meta.RecordMeta[0].BlockDigest)
	assert.Equal("sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", reply.Meta.RecordMeta[0].PayloadDigest)
	assert.Equal("c2_2000101002", reply.Meta.RecordMeta[0].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[0].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d$`, reply.Meta.RecordMeta[0].StorageRef)

	assert.Equal(int32(1), reply.Meta.RecordMeta[1].RecordNum)
	assert.Equal(contentwriter.RecordType_RESPONSE, reply.Meta.RecordMeta[1].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[1].WarcId)
	assert.Equal("sha1:9D3D1E0589A62F6091F53482A843399B66926655", reply.Meta.RecordMeta[1].BlockDigest)
	assert.Equal("sha1:C37FFB221569C553A2476C22C7DAD429F3492977", reply.Meta.RecordMeta[1].PayloadDigest)
	assert.Equal("c2_2000101002", reply.Meta.RecordMeta[1].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[1].RevisitReferenceId)
	assert.Regexp("warcfile:"+fileNamePattern+`:\d\d\d$`, reply.Meta.RecordMeta[1].StorageRef)

	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+".open$", 1)
	serverAndClient.close()
	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+"$", 1)
}

func TestContentWriterService_WriteRevisit(t *testing.T) {
	testSettings := settings.NewMock(t.TempDir(), 1)

	now = func() time.Time {
		return time.Date(2000, 10, 10, 2, 59, 59, 0, time.UTC)
	}

	serverAndClient := newServerAndClient(testSettings)
	serverAndClient.dbMock.
		On(r.Table("crawled_content").Get("sha1:C37FFB221569C553A2476C22C7DAD429F3492977:c1_2000101002")).
		Return(map[string]interface{}{
			"date":      time.Date(2021, 8, 27, 13, 52, 0, 0, time.UTC),
			"digest":    "digest",
			"targetUri": "http://www.example.com",
			"warcId":    "fff232109-0d71-467f-b728-de86be386c6f",
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

	fileNamePattern := `c1_2000101002-\d{14}-0001-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|.+).warc`

	assert.Equal(int32(0), reply.Meta.RecordMeta[0].RecordNum)
	assert.Equal(contentwriter.RecordType_REQUEST, reply.Meta.RecordMeta[0].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[0].WarcId)
	assert.Equal("sha1:AD6944346BF47CEACBE14E387EB031FCBDB59227", reply.Meta.RecordMeta[0].BlockDigest)
	assert.Equal("sha1:DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", reply.Meta.RecordMeta[0].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[0].CollectionFinalName)
	assert.Equal("", reply.Meta.RecordMeta[0].RevisitReferenceId)
	assert.Regexp(`warcfile:`+fileNamePattern+`:\d\d\d`, reply.Meta.RecordMeta[0].StorageRef)

	assert.Equal(int32(1), reply.Meta.RecordMeta[1].RecordNum)
	assert.Equal(contentwriter.RecordType_REVISIT, reply.Meta.RecordMeta[1].Type)
	assert.Regexp(".{8}-.{4}-.{4}-.{4}-.{12}", reply.Meta.RecordMeta[1].WarcId)
	assert.Equal("sha1:D5DBF98230700AAF71092C5655F7B55C3DA4CD69", reply.Meta.RecordMeta[1].BlockDigest)
	assert.Equal("sha1:C37FFB221569C553A2476C22C7DAD429F3492977", reply.Meta.RecordMeta[1].PayloadDigest)
	assert.Equal("c1_2000101002", reply.Meta.RecordMeta[1].CollectionFinalName)
	assert.Equal("<urn:uuid:fff232109-0d71-467f-b728-de86be386c6f>", reply.Meta.RecordMeta[1].RevisitReferenceId)
	assert.Regexp(`warcfile:`+fileNamePattern+`:\d\d\d\d`, reply.Meta.RecordMeta[1].StorageRef)

	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+".open$", 1)
	serverAndClient.close()
	dirHasFilesMatching(t, testSettings.WarcDir(), "^"+fileNamePattern+"$", 1)
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
