package dynamodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
	"google.golang.org/protobuf/proto"
)

// DynamoDurableStore implements the DurableStore interface
// and helps persist states in a DynamoDB
type DynamoDurableStore struct {
	ddb           database
	shuttingDown  bool
	lastStateSync *sync.Map
}

// enforce interface implementation
var _ persistence.StateStore = (*DynamoDurableStore)(nil)

func NewDurableStore(tableName string, client *dynamodb.Client) *DynamoDurableStore {
	syncMap := sync.Map{}
	return &DynamoDurableStore{
		ddb:           newDynamodb(tableName, client),
		shuttingDown:  false,
		lastStateSync: &syncMap,
	}
}

// Connect connects to the journal store
// No connection is needed because the client is stateless
func (DynamoDurableStore) Connect(_ context.Context) error {
	return nil
}

// Disconnect disconnect the journal store
// There is no need to disconnect because the client is stateless
func (DynamoDurableStore) Disconnect(_ context.Context) error {
	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
// There is no need to ping because the client is stateless
func (DynamoDurableStore) Ping(_ context.Context) error {
	return nil
}

// WriteState persist durable state for a given persistenceID.
func (s DynamoDurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	if !s.shuttingDown {
		lastSyncTime, ok := s.lastStateSync.Load(state.PersistenceId)
		if ok {
			syncTime, ok := lastSyncTime.(time.Time)
			if !ok {
				return fmt.Errorf("failed to cast last sync time")
			}
			if time.Since(syncTime) < 10*time.Second {
				return nil
			}
		}
	}

	s.lastStateSync.Store(state.GetPersistenceId(), time.Now())
	bytea, _ := proto.Marshal(state.GetResultingState())
	manifest := string(state.GetResultingState().ProtoReflect().Descriptor().FullName())

	return s.ddb.UpsertItem(ctx, &item{
		PersistenceID: state.GetPersistenceId(),
		VersionNumber: state.GetVersionNumber(),
		StatePayload:  bytea,
		StateManifest: manifest,
		Timestamp:     state.GetTimestamp(),
		ShardNumber:   state.GetShard(),
	})
}

// GetLatestState fetches the latest durable state
func (s DynamoDurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	result, err := s.ddb.GetItem(ctx, persistenceID)
	switch {
	case result == nil && err == nil:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		return result.ToDurableState()
	}
}

func (s DynamoDurableStore) Shutdown() {
	s.shuttingDown = true
}
