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
	s := &GrpcServer{
		listenHost:  host,
		listenPort:  port,
		settings:    settings,
		configCache: configCache,
		service: &ContentWriterService{
			warcWriterRegistry: newWarcWriterRegistry(settings),
			configCache:        configCache,
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
	s.service = &ContentWriterService{
		warcWriterRegistry: newWarcWriterRegistry(s.settings),
		configCache:        s.configCache,
	}
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
}

func (s *ContentWriterService) Write(stream contentwriter.ContentWriter_WriteServer) error {
	telemetry.ScopechecksTotal.Inc()
	//telemetry.ScopecheckResponseTotal.With(prometheus.Labels{"code": strconv.Itoa(int(result.ExcludeReason))}).Inc()
	ctx := newWriteSessionContext(s.configCache)

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return s.onCompleted(ctx, stream)
			//return stream.SendAndClose(&contentwriter.WriteReply{
			//	Meta: &contentwriter.WriteResponseMeta{
			//		RecordMeta: map[int32](*contentwriter.WriteResponseMeta_RecordMeta){
			//			0: {
			//				RecordNum:           0,
			//				Type:                0,
			//				WarcId:              "",
			//				StorageRef:          "",
			//				BlockDigest:         "",
			//				PayloadDigest:       "",
			//				RevisitReferenceId:  "",
			//				CollectionFinalName: "",
			//			},
			//		},
			//	},
			//})
		}
		if err != nil {
			log.Err(err).Msgf("Error caught: %s", err.Error())
			ctx.cancelSession(err.Error())
			return err
		}

		switch v := request.Value.(type) {
		case *contentwriter.WriteRequest_Meta:
			if err := ctx.setWriteRequestMeta(v.Meta); err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
		case *contentwriter.WriteRequest_ProtocolHeader:
			recordBuilder, err := ctx.getRecordBuilder(v.ProtocolHeader.RecordNum)
			if err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
			if recordBuilder.Size() != 0 {
				ctx.cancelSession(err.Error())
				return ctx.handleErr(codes.InvalidArgument, "Header received twice")
			}
			if _, err := recordBuilder.Write(v.ProtocolHeader.GetData()); err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
			if _, err := recordBuilder.Write([]byte("\n\n")); err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
		case *contentwriter.WriteRequest_Payload:
			recordBuilder, err := ctx.getRecordBuilder(v.Payload.RecordNum)
			if err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
			if _, err := recordBuilder.Write(v.Payload.GetData()); err != nil {
				ctx.cancelSession(err.Error())
				return err
			}
		case *contentwriter.WriteRequest_Cancel:
			ctx.cancelSession(v.Cancel)
		default:
			return fmt.Errorf("Invalid request %s", v)
		}
	}
}

func (s *ContentWriterService) onCompleted(context *writeSessionContext, stream contentwriter.ContentWriter_WriteServer) error {
	if context.canceled {
		return stream.SendAndClose(&contentwriter.WriteReply{})
	}

	if context.meta == nil {
		return context.handleErr(codes.InvalidArgument, "Missing metadata object")
	}

	reply := &contentwriter.WriteReply{
		Meta: &contentwriter.WriteResponseMeta{
			RecordMeta: make(map[int32]*contentwriter.WriteResponseMeta_RecordMeta),
		},
	}

	if err := context.validateSession(); err != nil {
		context.cancelSession("Validation failed: " + err.Error())
		return err
	}

	for n, m := range context.meta.RecordMeta {
		// TODO: Detect revisit
		record := context.records[n]
		writer := s.warcWriterRegistry.GetWarcWriter(context.collectionConfig, m)
		writer.fileWriter.Write(record)
		reply.Meta.RecordMeta[n] = &contentwriter.WriteResponseMeta_RecordMeta{
			RecordNum:           n,
			Type:                0,
			WarcId:              record.WarcHeader().Get(gowarc.WarcRecordID),
			StorageRef:          "",
			BlockDigest:         record.WarcHeader().Get(gowarc.WarcBlockDigest),
			PayloadDigest:       record.WarcHeader().Get(gowarc.WarcPayloadDigest),
			RevisitReferenceId:  "",
			CollectionFinalName: "",
		}
	}

	return stream.SendAndClose(reply)
	//WarcCollection	collection = warcCollectionRegistry.getWarcCollection(context.getCollectionConfig())
	//try(Instance	warcWriters = collection.getWarcWriters()) {
	//	for Integer
	//recordNum:
	//	context.getRecordNums()) {
	//		try(RecordData			recordData = context.getRecordData(recordNum)) {
	//			context.detectRevisit(recordNum, collection)
	//
	//			URI				ref = warcWriters.getWarcWriter(recordData.getSubCollectionType()).writeRecord(recordData)
	//
	//			WriteResponseMeta.RecordMeta.Builder				responseMeta = WriteResponseMeta.RecordMeta.newBuilder()
	//			.setRecordNum(recordNum)
	//			.setType(recordData.getRecordType())
	//			.setWarcId(recordData.getWarcId())
	//			.setStorageRef(ref.toString())
	//			.setBlockDigest(recordData.getContentBuffer().getBlockDigest())
	//			.setPayloadDigest(recordData.getContentBuffer().getPayloadDigest())
	//			.setCollectionFinalName(collection.getCollectionName(recordData.getSubCollectionType()))
	//			if recordData.getRevisitRef() != null {
	//				responseMeta.setRevisitReferenceId(recordData.getRevisitRef().getWarcId())
	//			}
	//
	//			reply.getMetaBuilder().putRecordMeta(responseMeta.getRecordNum(), responseMeta.build())
	//		}
	//		catch(IOException			ex) {
	//			Status				status = Status.UNKNOWN.withDescription(ex.toString())
	//			LOG.error("Failed write: {}", ex.getMessage(), ex)
	//			responseObserver.onError(status.asException())
	//		}
	//		catch(SingleWarcWriter.SizeMismatchException			ex) {
	//			Status
	//			status = Status.OUT_OF_RANGE.withDescription(ex.getMessage())
	//			LOG.error(status.getDescription())
	//			throw				status.asException()
	//		}
	//		catch(Exception			ex) {
	//			LOG.error("Failed write: {}", ex.getMessage(), ex)
	//			responseObserver.onError(Status.fromThrowable(ex).asException())
	//		}
	//	}
	//	responseObserver.onNext(reply.build())
	//	responseObserver.onCompleted()
	//}
	//catch(Exception	ex) {
	//	Status		status = Status.UNKNOWN.withDescription(ex.toString())
	//	LOG.error(ex.getMessage(), ex)
	//	responseObserver.onError(status.asException())
	//}
}
