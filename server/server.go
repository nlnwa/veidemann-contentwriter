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
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"google.golang.org/grpc/codes"
	"io"

	"github.com/nlnwa/veidemann-api/go/contentwriter/v1"
	"github.com/nlnwa/veidemann-contentwriter/telemetry"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
)

type GrpcServer struct {
	listenHost  string
	listenPort  int
	settings    settings.Settings
	grpcServer  *grpc.Server
	configCache database.ConfigCache
	service     *ContentWriterService
}

func New(host string, port int, settings settings.Settings, configCache database.ConfigCache) *GrpcServer {
	recordOpts := []gowarc.WarcRecordOption{
		gowarc.WithBufferTmpDir(settings.WorkDir()),
		gowarc.WithVersion(settings.WarcVersion()),
	}
	if settings.UseStrictValidation() {
		recordOpts = append(recordOpts, gowarc.WithStrictValidation())
	}
	s := &GrpcServer{
		listenHost:  host,
		listenPort:  port,
		settings:    settings,
		configCache: configCache,
		service: &ContentWriterService{
			warcWriterRegistry: newWarcWriterRegistry(settings, configCache),
			configCache:        configCache,
			recordOptions:      recordOpts,
		},
	}
	return s
}

func (s *GrpcServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.listenHost, s.listenPort))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	tracer := opentracing.GlobalTracer()
	var opts = []grpc.ServerOption{
		grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(tracer)),
	}
	s.grpcServer = grpc.NewServer(opts...)
	contentwriter.RegisterContentWriterServer(s.grpcServer, s.service)

	log.Info().Msgf("ContentWriter Service listening on %s", lis.Addr())
	return s.grpcServer.Serve(lis)
}

func (s *GrpcServer) Shutdown() {
	log.Info().Msg("Shutting down ContentWriter Service")
	s.grpcServer.GracefulStop()
	s.service.warcWriterRegistry.Shutdown()
}

type ContentWriterService struct {
	contentwriter.UnimplementedContentWriterServer
	configCache        database.ConfigCache
	warcWriterRegistry *warcWriterRegistry
	recordOptions      []gowarc.WarcRecordOption
}

func (s *ContentWriterService) Write(stream contentwriter.ContentWriter_WriteServer) error {
	telemetry.ScopechecksTotal.Inc()
	//telemetry.ScopecheckResponseTotal.With(prometheus.Labels{"code": strconv.Itoa(int(result.ExcludeReason))}).Inc()
	ctx := newWriteSessionContext(s.configCache, s.recordOptions)

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return s.onCompleted(ctx, stream)
		}
		if err != nil {
			log.Err(err).Msgf("Error caught: %s", err.Error())
			ctx.cancelSession(err.Error())
			return err
		}

		switch v := request.Value.(type) {
		case *contentwriter.WriteRequest_Meta:
			log.Trace().Msgf("Got API request %T for %d records", v, len(v.Meta.RecordMeta))
			if err := ctx.setWriteRequestMeta(v.Meta); err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
		case *contentwriter.WriteRequest_ProtocolHeader:
			log.Trace().Msgf("Got API request %T for record #%d. Size: %d", v, v.ProtocolHeader.RecordNum, len(v.ProtocolHeader.GetData()))
			if err := ctx.writeProtocolHeader(v.ProtocolHeader); err != nil {
				return err
			}
		case *contentwriter.WriteRequest_Payload:
			log.Trace().Msgf("Got API request %T for record #%d. Size: %d", v, v.Payload.RecordNum, len(v.Payload.GetData()))
			if err := ctx.writePayoad(v.Payload); err != nil {
				return err
			}
		case *contentwriter.WriteRequest_Cancel:
			log.Trace().Msgf("Got API request %T", v)
			ctx.cancelSession(v.Cancel)
		default:
			return fmt.Errorf("Invalid request %s", v)
		}
	}
}

func (s *ContentWriterService) onCompleted(context *writeSessionContext, stream contentwriter.ContentWriter_WriteServer) error {
	if context.canceled {
		return context.handleErr(codes.Canceled, "Session canceled")
		//return stream.SendAndClose(&contentwriter.WriteReply{})
	}

	if context.meta == nil {
		return context.handleErr(codes.InvalidArgument, "Missing metadata object")
	}

	if err := context.validateSession(); err != nil {
		context.cancelSession("Validation failed: " + err.Error())
		return err
	}

	records := make([]gowarc.WarcRecord, len(context.records))
	for i := 0; i < len(records); i++ {
		records[i] = context.records[int32(i)]
	}
	writer := s.warcWriterRegistry.GetWarcWriter(context.collectionConfig, context.meta.RecordMeta[0])
	writeResponseMeta, err := writer.Write(context.meta, records...)
	if err != nil {
		context.cancelSession("Failed writing record: " + err.Error())
		return err
	}

	return stream.SendAndClose(writeResponseMeta)
}
