package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
	"google.golang.org/protobuf/proto"
)

// DynamoDurableStore implements the DurableStore interface
// and helps persist states in a DynamoDB
type DynamoDurableStore struct {
	ddb        database
	writerChan chan *egopb.DurableState
}

// enforce interface implementation
var _ persistence.StateStore = (*DynamoDurableStore)(nil)

func NewDurableStore(tableName string, client *dynamodb.Client) *DynamoDurableStore {
	s := &DynamoDurableStore{
		ddb:        newDynamodb(tableName, client),
		writerChan: make(chan *egopb.DurableState, 100),
	}

	// Run ticker loop that flushes the writes to the database
	go s.flushWrites()

	return s
}

func (s *DynamoDurableStore) flushWrites() {
	ctx := context.Background()

	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Consuming messages...")
		allStates := make(map[string]*egopb.DurableState)

	Writerloop:
		// Drain the channel until empty
		for {
			select {
			case msg := <-s.writerChan:
				allStates[msg.GetPersistenceId()] = msg
				fmt.Println("Consumed msg:", msg)
			default:
				fmt.Println("No more messages to consume.")
				if len(allStates) > 0 {
					fmt.Println("Flushing all states to db:", len(allStates))
				}

				for _, msg := range allStates {
					s.writeState(ctx, msg)
					delete(allStates, msg.GetPersistenceId())
				}

				break Writerloop
			}
		}
	}
}

// Connect connects to the journal store
// No connection is needed because the client is stateless
func (DynamoDurableStore) Connect(_ context.Context) error {
	return nil
}

// Disconnect disconnect the journal store
// There is no need to disconnect because the client is stateless
func (s DynamoDurableStore) Disconnect(ctx context.Context) error {
	// Flush all the data
	fmt.Println("Flushing all data... before disconnecting")
	for {
		select {
		case msg := <-s.writerChan:
			s.writeState(ctx, msg)
		default:
			fmt.Println("No more messages to consume.")
			return nil
		}
	}
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
// There is no need to ping because the client is stateless
func (DynamoDurableStore) Ping(_ context.Context) error {
	return nil
}

// WriteState writes the state to the channel and immediately returns.
func (s DynamoDurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	fmt.Println("Writing state to chan:", state.GetPersistenceId(), state.GetVersionNumber())
	s.writerChan <- state
	return nil
}

// writeState actually persist durable state for a given persistenceID.
func (s DynamoDurableStore) writeState(ctx context.Context, state *egopb.DurableState) error {
	fmt.Println("Writing state to db:", state.GetPersistenceId(), state.GetVersionNumber())
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
