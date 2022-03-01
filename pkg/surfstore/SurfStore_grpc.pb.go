// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package surfstore

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// BlockStoreClient is the client API for BlockStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BlockStoreClient interface {
	GetBlock(ctx context.Context, in *BlockHash, opts ...grpc.CallOption) (*Block, error)
	PutBlock(ctx context.Context, in *Block, opts ...grpc.CallOption) (*Success, error)
	HasBlocks(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockHashes, error)
}

type blockStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewBlockStoreClient(cc grpc.ClientConnInterface) BlockStoreClient {
	return &blockStoreClient{cc}
}

func (c *blockStoreClient) GetBlock(ctx context.Context, in *BlockHash, opts ...grpc.CallOption) (*Block, error) {
	out := new(Block)
	err := c.cc.Invoke(ctx, "/surfstore.BlockStore/GetBlock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blockStoreClient) PutBlock(ctx context.Context, in *Block, opts ...grpc.CallOption) (*Success, error) {
	out := new(Success)
	err := c.cc.Invoke(ctx, "/surfstore.BlockStore/PutBlock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *blockStoreClient) HasBlocks(ctx context.Context, in *BlockHashes, opts ...grpc.CallOption) (*BlockHashes, error) {
	out := new(BlockHashes)
	err := c.cc.Invoke(ctx, "/surfstore.BlockStore/HasBlocks", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BlockStoreServer is the server API for BlockStore service.
// All implementations must embed UnimplementedBlockStoreServer
// for forward compatibility
type BlockStoreServer interface {
	GetBlock(context.Context, *BlockHash) (*Block, error)
	PutBlock(context.Context, *Block) (*Success, error)
	HasBlocks(context.Context, *BlockHashes) (*BlockHashes, error)
	mustEmbedUnimplementedBlockStoreServer()
}

// UnimplementedBlockStoreServer must be embedded to have forward compatible implementations.
type UnimplementedBlockStoreServer struct {
}

func (UnimplementedBlockStoreServer) GetBlock(context.Context, *BlockHash) (*Block, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlock not implemented")
}
func (UnimplementedBlockStoreServer) PutBlock(context.Context, *Block) (*Success, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutBlock not implemented")
}
func (UnimplementedBlockStoreServer) HasBlocks(context.Context, *BlockHashes) (*BlockHashes, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HasBlocks not implemented")
}
func (UnimplementedBlockStoreServer) mustEmbedUnimplementedBlockStoreServer() {}

// UnsafeBlockStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BlockStoreServer will
// result in compilation errors.
type UnsafeBlockStoreServer interface {
	mustEmbedUnimplementedBlockStoreServer()
}

func RegisterBlockStoreServer(s grpc.ServiceRegistrar, srv BlockStoreServer) {
	s.RegisterService(&BlockStore_ServiceDesc, srv)
}

func _BlockStore_GetBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockHash)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).GetBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.BlockStore/GetBlock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).GetBlock(ctx, req.(*BlockHash))
	}
	return interceptor(ctx, in, info, handler)
}

func _BlockStore_PutBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Block)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).PutBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.BlockStore/PutBlock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).PutBlock(ctx, req.(*Block))
	}
	return interceptor(ctx, in, info, handler)
}

func _BlockStore_HasBlocks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockHashes)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BlockStoreServer).HasBlocks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.BlockStore/HasBlocks",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BlockStoreServer).HasBlocks(ctx, req.(*BlockHashes))
	}
	return interceptor(ctx, in, info, handler)
}

// BlockStore_ServiceDesc is the grpc.ServiceDesc for BlockStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BlockStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "surfstore.BlockStore",
	HandlerType: (*BlockStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetBlock",
			Handler:    _BlockStore_GetBlock_Handler,
		},
		{
			MethodName: "PutBlock",
			Handler:    _BlockStore_PutBlock_Handler,
		},
		{
			MethodName: "HasBlocks",
			Handler:    _BlockStore_HasBlocks_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/surfstore/SurfStore.proto",
}

// MetaStoreClient is the client API for MetaStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MetaStoreClient interface {
	GetFileInfoMap(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*FileInfoMap, error)
	UpdateFile(ctx context.Context, in *FileMetaData, opts ...grpc.CallOption) (*Version, error)
	GetBlockStoreAddr(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockStoreAddr, error)
}

type metaStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftSurfstoreClient(cc grpc.ClientConnInterface) MetaStoreClient {
	return &metaStoreClient{cc}
}

func (c *metaStoreClient) GetFileInfoMap(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*FileInfoMap, error) {
	out := new(FileInfoMap)
	err := c.cc.Invoke(ctx, "/surfstore.MetaStore/GetFileInfoMap", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *metaStoreClient) UpdateFile(ctx context.Context, in *FileMetaData, opts ...grpc.CallOption) (*Version, error) {
	out := new(Version)
	err := c.cc.Invoke(ctx, "/surfstore.MetaStore/UpdateFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *metaStoreClient) GetBlockStoreAddr(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*BlockStoreAddr, error) {
	out := new(BlockStoreAddr)
	err := c.cc.Invoke(ctx, "/surfstore.MetaStore/GetBlockStoreAddr", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MetaStoreServer is the server API for MetaStore service.
// All implementations must embed UnimplementedMetaStoreServer
// for forward compatibility
type MetaStoreServer interface {
	GetFileInfoMap(context.Context, *emptypb.Empty) (*FileInfoMap, error)
	UpdateFile(context.Context, *FileMetaData) (*Version, error)
	GetBlockStoreAddr(context.Context, *emptypb.Empty) (*BlockStoreAddr, error)
	mustEmbedUnimplementedMetaStoreServer()
}

// UnimplementedMetaStoreServer must be embedded to have forward compatible implementations.
type UnimplementedMetaStoreServer struct {
}

func (UnimplementedMetaStoreServer) GetFileInfoMap(context.Context, *emptypb.Empty) (*FileInfoMap, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFileInfoMap not implemented")
}
func (UnimplementedMetaStoreServer) UpdateFile(context.Context, *FileMetaData) (*Version, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateFile not implemented")
}
func (UnimplementedMetaStoreServer) GetBlockStoreAddr(context.Context, *emptypb.Empty) (*BlockStoreAddr, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlockStoreAddr not implemented")
}
func (UnimplementedMetaStoreServer) mustEmbedUnimplementedMetaStoreServer() {}

// UnsafeMetaStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MetaStoreServer will
// result in compilation errors.
type UnsafeMetaStoreServer interface {
	mustEmbedUnimplementedMetaStoreServer()
}

func RegisterMetaStoreServer(s grpc.ServiceRegistrar, srv MetaStoreServer) {
	s.RegisterService(&MetaStore_ServiceDesc, srv)
}

func _MetaStore_GetFileInfoMap_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).GetFileInfoMap(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.MetaStore/GetFileInfoMap",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).GetFileInfoMap(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _MetaStore_UpdateFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FileMetaData)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).UpdateFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.MetaStore/UpdateFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).UpdateFile(ctx, req.(*FileMetaData))
	}
	return interceptor(ctx, in, info, handler)
}

func _MetaStore_GetBlockStoreAddr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetaStoreServer).GetBlockStoreAddr(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/surfstore.MetaStore/GetBlockStoreAddr",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MetaStoreServer).GetBlockStoreAddr(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// MetaStore_ServiceDesc is the grpc.ServiceDesc for MetaStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MetaStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "surfstore.MetaStore",
	HandlerType: (*MetaStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFileInfoMap",
			Handler:    _MetaStore_GetFileInfoMap_Handler,
		},
		{
			MethodName: "UpdateFile",
			Handler:    _MetaStore_UpdateFile_Handler,
		},
		{
			MethodName: "GetBlockStoreAddr",
			Handler:    _MetaStore_GetBlockStoreAddr_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/surfstore/SurfStore.proto",
}
