package store

import (
	"fmt"
	"reflect"

	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// Individual parameter store for each keeper
// Transient store persists for a block, so we use it for
// recording whether the parameter has been changed or not
type Store struct {
	cdc  *codec.Codec
	key  sdk.StoreKey // []byte -> []byte, stores parameter
	tkey sdk.StoreKey // []byte -> bool, stores parameter change

	space []byte
}

// NewStore constructs a store with namestore
func NewStore(cdc *codec.Codec, key sdk.StoreKey, tkey sdk.StoreKey, space string) Store {
	return Store{
		cdc:  cdc,
		key:  key,
		tkey: tkey,

		space: []byte(space),
	}
}

// Returns a KVStore identical with ctx,TransientStore(s.key).Prefix()
func (s Store) kvStore(ctx sdk.Context) sdk.KVStore {
	return ctx.KVStore(s.key).Prefix(append(s.space, '/'))
}

// Returns a KVStore identical with ctx.TransientStore(s.tkey).Prefix()
func (s Store) transientStore(ctx sdk.Context) sdk.KVStore {
	return ctx.TransientStore(s.tkey).Prefix(append(s.space, '/'))
}

// Get parameter from store
func (s Store) Get(ctx sdk.Context, key []byte, ptr interface{}) {
	store := s.kvStore(ctx)
	bz := store.Get(key)
	err := s.cdc.UnmarshalJSON(bz, ptr)
	if err != nil {
		panic(err)
	}
}

// GetIfExists do not modify ptr if the stored parameter is nil
func (s Store) GetIfExists(ctx sdk.Context, key []byte, ptr interface{}) {
	store := s.kvStore(ctx)
	bz := store.Get(key)
	if bz == nil {
		return
	}
	err := s.cdc.UnmarshalJSON(bz, ptr)
	if err != nil {
		panic(err)
	}
}

// Get raw bytes of parameter from store
func (s Store) GetRaw(ctx sdk.Context, key []byte) []byte {
	store := s.kvStore(ctx)
	res := store.Get(key)
	return res
}

// Check if the parameter is set in the store
func (s Store) Has(ctx sdk.Context, key []byte) bool {
	store := s.kvStore(ctx)
	return store.Has(key)
}

// Returns true if the parameter is set in the block
func (s Store) Modified(ctx sdk.Context, key []byte) bool {
	tstore := s.transientStore(ctx)
	return tstore.Has(key)
}

// Set parameter, return error if stored parameter has different type from input
// Also set to the transient store to record change
func (s Store) Set(ctx sdk.Context, key []byte, param interface{}) {
	store := s.kvStore(ctx)

	bz := store.Get(key)

	// To prevent invalid parameter set, we check the type of the stored parameter
	// and try to match it with the provided parameter. It continues only if they matches.
	// It is possible because parameter set happens rarely.
	if bz != nil {
		ptrType := reflect.PtrTo(reflect.TypeOf(param))
		ptr := reflect.New(ptrType).Interface()

		if s.cdc.UnmarshalJSON(bz, ptr) != nil {
			panic(fmt.Errorf("Type mismatch with stored param and provided param"))
		}
	}

	bz, err := s.cdc.MarshalJSON(param)
	if err != nil {
		panic(err)
	}
	store.Set(key, bz)

	tstore := s.transientStore(ctx)
	tstore.Set(key, []byte{})
}

// Set raw bytes of parameter
// Also set to the transient store to record change
func (s Store) SetRaw(ctx sdk.Context, key []byte, param []byte) {
	store := s.kvStore(ctx)
	store.Set(key, param)

	tstore := s.transientStore(ctx)
	tstore.Set(key, []byte{})
}

// Get to ParamStruct
func (s Store) GetStruct(ctx sdk.Context, ps ParamStruct) {
	for _, pair := range ps.KeyFieldPairs() {
		s.Get(ctx, pair.Key, pair.Field)
	}
}

// Set from ParamStruct
func (s Store) SetStruct(ctx sdk.Context, ps ParamStruct) {
	for _, pair := range ps.KeyFieldPairs() {
		// pair.Field is a pointer to the field, so indirecting the ptr.
		// go-amino automatically handles it but just for sure,
		// since SetStruct is meant to be used in InitGenesis
		// so this method will not be called frequently
		v := reflect.Indirect(reflect.ValueOf(pair.Field)).Interface()
		s.Set(ctx, pair.Key, v)
	}
}

// Returns internal namespace
func (s Store) Space() string {
	return string(s.space)
}

// Wrapper of Store, provides immutable functions only
type ReadOnlyStore struct {
	s Store
}

// Exposes Get
func (ros ReadOnlyStore) Get(ctx sdk.Context, key []byte, ptr interface{}) {
	ros.s.Get(ctx, key, ptr)
}

// Exposes GetRaw
func (ros ReadOnlyStore) GetRaw(ctx sdk.Context, key []byte) []byte {
	return ros.s.GetRaw(ctx, key)
}

// Exposes Has
func (ros ReadOnlyStore) Has(ctx sdk.Context, key []byte) bool {
	return ros.s.Has(ctx, key)
}

// Exposes Modified
func (ros ReadOnlyStore) Modified(ctx sdk.Context, key []byte) bool {
	return ros.s.Modified(ctx, key)
}

// Exposes Space
func (ros ReadOnlyStore) Space() string {
	return ros.s.Space()
}