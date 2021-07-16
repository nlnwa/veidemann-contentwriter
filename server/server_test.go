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
	"github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func mockServer(t *testing.T, c database.ConfigCache) *GrpcServer {
	lis = bufconn.Listen(bufSize)
	s := New("", 0, &MockSettings{warcWriterPoolSize: 1}, c)
	s.grpcServer = grpc.NewServer()
	contentwriter.RegisterContentWriterServer(s.grpcServer, s.service)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			t.Fatalf("Server exited with error: %v", err)
		}
	}()
	return s
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

type mockConfigCache struct{}

func (m mockConfigCache) GetConfigObject(ctx context.Context, ref *config.ConfigRef) (*config.ConfigObject, error) {
	fmt.Printf(":-) %s\n", ref)
	switch ref.Id {
	case "c1":
		return &config.ConfigObject{
			Id:   ref.Id,
			Meta: &config.Meta{Name: ref.Id},
			Spec: &config.ConfigObject_Collection{&config.Collection{
				CollectionDedupPolicy: 0,
				FileRotationPolicy:    0,
				Compress:              false,
				FileSize:              0,
				SubCollections:        nil,
			},
			}}, nil
	}
	return nil, nil
}

type MockSettings struct {
	hostName                      string
	warcDir                       string
	warcWriterPoolSize            int
	workDir                       string
	terminationGracePeriodSeconds int
}

func (m MockSettings) HostName() string {
	return m.hostName
}

func (m MockSettings) WarcDir() string {
	return m.warcDir
}

func (m MockSettings) WarcWriterPoolSize() int {
	return m.warcWriterPoolSize
}

func (m MockSettings) WorkDir() string {
	return m.workDir
}

func (m MockSettings) TerminationGracePeriodSeconds() int {
	return m.terminationGracePeriodSeconds
}

func TestContentWriterService_Write(t *testing.T) {
	type args []*contentwriter.WriteRequest
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"1",
			args{
				&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Meta{Meta: &contentwriter.WriteRequestMeta{
					ExecutionId: "eid1",
					TargetUri:   "",
					RecordMeta: map[int32]*contentwriter.WriteRequestMeta_RecordMeta{
						0: {
							RecordNum:         0,
							Type:              contentwriter.RecordType_RESPONSE,
							Size:              257,
							RecordContentType: "application/http;msgtype=response",
							BlockDigest:       "sha1:B285747AD7CC57AA74BCE2E30B453C8D1CB71BA4",
						},
					},
					FetchTimeStamp: nil,
					IpAddress:      "127.0.0.1",
					CollectionRef:  &config.ConfigRef{Kind: config.Kind_collection, Id: "c1"},
				}}},
				&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_ProtocolHeader{ProtocolHeader: &contentwriter.Data{
					RecordNum: 0,
					Data: []byte(
						"HTTP/1.1 200 OK\nDate: Tue, 19 Sep 2016 17:18:40 GMT\nServer: Apache/2.0.54 (Ubuntu)\n" +
							"Last-Modified: Mon, 16 Jun 2013 22:28:51 GMT\nETag: \"3e45-67e-2ed02ec0\"\nAccept-Ranges: bytes\n" +
							"Content-Length: 19\nConnection: close\nContent-Type: text/plain",
					),
				}}},
				&contentwriter.WriteRequest{Value: &contentwriter.WriteRequest_Payload{Payload: &contentwriter.Data{
					RecordNum: 0,
					Data:      []byte("This is the content"),
				}}},
			},
			false,
		},
	}

	// Set up test server
	server := mockServer(t, &mockConfigCache{})
	defer server.Shutdown()

	// Set up client
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close() //nolint
	client := contentwriter.NewContentWriterClient(conn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			stream, err := client.Write(ctx)
			assert.NoError(err)
			for i, r := range tt.args {
				err = stream.Send(r)
				assert.NoErrorf(err, "Error sending request #%d", i)
			}
			reply, err := stream.CloseAndRecv()
			assert.NoError(err)
			fmt.Printf("%v\n", reply)
		})
	}
}
