package eth_oracle

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/log"
	"github.com/kwilteam/kwil-db/core/types/serialize"
	"github.com/kwilteam/kwil-db/extensions/hooks"
	"github.com/kwilteam/kwil-db/extensions/listeners"
	"github.com/kwilteam/kwil-db/extensions/resolutions"
)

// this implements a Kwil event listener, which makes it easier to subscribe to event signatures
// from EVM contracts.

type EthListener struct {
	// ContractAddresses is a list of contract addresses to listen to events from.
	ContractAddresses []string
	// EventSignatures is a list of event signatures to listen to.
	// All events from any contract configured matching any of these signatures will be emitted.
	// It is optional and defaults to all events.
	EventSignatures []string
	// StartHeight is the block height to start syncing from when the node starts.
	// It can be used to skip syncing parts of the chain that have already been processed.
	// It is optional and defaults to 0.
	StartHeight uint64
	// ExtensionName is the unique name of the extension.
	// It is optional and defaults to "eth_listener".
	ExtensionName string
	// ConfigName is the name of the configuration for the listener.
	// It is optional and defaults to "eth_listener".
	ConfigName string
	// RequiredConfirmations is the number of confirmations required before an event is considered final.
	// It is optional and defaults to 12.
	RequiredConfirmations int64
	// RefundThreshold is the required vote percentage threshold for
	// all voters on a resolution to be refunded the gas costs
	// associated with voting. This allows for resolutions that have
	// not received enough votes to pass to refund gas to the voters
	// that have voted on the resolution. For a 1/3rds threshold,
	// >=1/3rds of the voters must vote for the resolution for
	// refunds to occur. If this threshold is not met, voters will
	// not be refunded when the resolution expires. The number must
	// be a fraction between 0 and 1. If this field is nil, it will
	// default to only refunding voters when the resolution is confirmed.
	RefundThreshold *big.Rat
	// ConfirmationThreshold is the required vote percentage
	// threshold for whether a resolution is confirmed. In a 2/3rds
	// threshold, >=2/3rds of the voters must vote for the resolution
	// for it to be confirmed. Voters will also be refunded if this
	// threshold is met, regardless of the refund threshold. The
	// number must be a fraction between 0 and 1. If this field is
	// nil, it will default to 2/3.
	ConfirmationThreshold *big.Rat
	// ExpirationPeriod is the amount of blocks that the resolution
	// will be valid for before it expires. It is applied additively
	// to the current block height when the resolution is proposed;
	// if the current block height is 10 and the expiration height is
	// 5, the resolution will expire at block 15. If this field is
	// <1, it will default to 14400, which is approximately 1 day
	// assuming 6 second blocks.
	ExpirationPeriod int64
	// Resolve is a function that resolves the event data post-consensus.
	Resolve func(ctx context.Context, app *common.App, log types.Log) error
}

// RegisterEthListener registers the Ethereum listener. It should be called in the init function.
func RegisterEthListener(l EthListener) {
	// fill in the defaults
	if len(l.ContractAddresses) == 0 {
		panic("no contract addresses provided")
	}

	if l.ExtensionName == "" {
		l.ExtensionName = "eth_listener"
	}

	if l.ConfigName == "" {
		l.ConfigName = "eth_listener"
	}

	if l.RequiredConfirmations < 0 {
		panic("required confirmations cannot be negative")
	}
	if l.RequiredConfirmations == 0 {
		l.RequiredConfirmations = 12
	}

	if l.Resolve == nil {
		panic("no resolve function provided")
	}

	err := listeners.RegisterListener(l.uniqueName("listener"), l.Listen)
	if err != nil {
		panic(fmt.Sprintf("failed to register listener: %s", err))
	}

	err = resolutions.RegisterResolution(l.resolutionExtName(), resolutions.ModAdd, l.resolutionConfig())
	if err != nil {
		panic(fmt.Sprintf("failed to register resolution: %s", err))
	}

	err = hooks.RegisterGenesisHook(l.uniqueName("genesis-hook"), l.genesisHook())
	if err != nil {
		panic(fmt.Sprintf("failed to register genesis hook: %s", err))
	}

	err = hooks.RegisterEndBlockHook(l.uniqueName("end-block-hook"), l.endBlockHook())
	if err != nil {
		panic(fmt.Sprintf("failed to register end block hook: %s", err))
	}
}

func (el EthListener) resolutionExtName() string {
	return el.uniqueName("resolution")
}

func (el EthListener) Listen(ctx context.Context, service *common.Service, eventstore listeners.EventStore) error {
	logger := service.Logger.Named("eth_listener." + el.ExtensionName)
	cfg := &Config{}

	cfgMap, ok := service.LocalConfig.AppConfig.Extensions[el.ConfigName]
	if !ok {
		logger.Warn("no configuration found for listener, not running...")
		return nil
	}

	err := cfg.setConfig(cfgMap)
	if err != nil {
		return fmt.Errorf(`failed to set config for extension "%s": %w`, el.ExtensionName, err)
	}

	startBlock, err := getLastSeenHeight(ctx, eventstore)
	if err != nil {
		return err
	}

	startBlock = bestHeight(int64(el.StartHeight), startBlock)
	logger.Info("starting listener from block", "block", startBlock)

	client, err := newEthClient(ctx, cfg.RPCURL, cfg.MaxRetries, el.ContractAddresses, el.EventSignatures, logger)
	if err != nil {
		return err
	}

	currentBlock, err := client.GetLatestBlock(ctx)
	if err != nil {
		return err
	}

	if currentBlock < startBlock {
		return fmt.Errorf("starting height is greater than the last confirmed eth block height")
	}

	lastConfirmedBlock := currentBlock - el.RequiredConfirmations

	// we will now sync all logs from the starting height to the current height,
	// in chunks of config.BlockSyncChunkSize
	for {
		if startBlock >= lastConfirmedBlock {
			break
		}

		toBlock := startBlock + cfg.BlockSyncChunkSize
		if toBlock > lastConfirmedBlock {
			toBlock = lastConfirmedBlock
		}

		err = el.processEvents(ctx, startBlock, toBlock, client, eventstore, logger)
		if err != nil {
			return err
		}

		startBlock = toBlock
	}

	service.Logger.Info("caught up with network", "from", el.StartHeight, "to", lastConfirmedBlock)

	outerErr := client.ListenToBlocks(ctx, time.Duration(cfg.ReconnectionInterval)*time.Second, func(newHeight int64) error {
		newHeight = newHeight - el.RequiredConfirmations

		// it is possible to receive the same block height multiple times
		if newHeight <= startBlock {
			logger.Debug("received duplicate block height", "block", newHeight)
			return nil
		}

		logger.Info("received new block", "block", newHeight)

		// lastheight + 1 because we have already processed the last height
		err := el.processEvents(ctx, startBlock+1, newHeight, client, eventstore, logger)
		if err != nil {
			return err
		}

		startBlock = newHeight

		return nil
	})

	if outerErr != nil {
		logger.Error("listener stopped with unexpected error, shutting down...", "error", outerErr)
	}

	return nil
}

// Config holds the configuration for the Ethereum listener.
// This allows individual node operators to configure the listener to their needs.
type Config struct {
	// RPCURL is the URL of the RPC to connect to.
	// It is required.
	RPCURL string
	// ReconnectionInterval is the amount of time in seconds that the listener
	// will wait before resubscribing for new Ethereum Blocks. Reconnects are
	// automatically handled, but a subscription may stall, in which case we
	// will make a new subscription. If the write or read on the connection to
	// the RPC provider errors, the RPC client will reconnect, and we will
	// continue to reestablish a new block subscription. If not configured, it
	// will default to 60s.
	ReconnectionInterval int64
	// MaxRetries is the total number of times the listener will attempt an RPC
	// with the provider before giving up. It will exponentially back off after
	// each try, starting at 1 second and doubling each time. If not configured,
	// it will default to 10.
	MaxRetries int64
	// BlockSyncChunkSize is the number of Ethereum blocks the listener will request from the
	// Ethereum RPC endpoint at a time while catching up to the network. If not configured,
	// it will default to 1,000,000.
	BlockSyncChunkSize int64
}

// setConfig sets the default values for the Config struct.
// It accesses them from a map, which is standard for Kwil's extension configuration.
func (c *Config) setConfig(m map[string]string) error {
	if v, ok := m["rpc_url"]; ok {
		c.RPCURL = v
	} else {
		return errors.New("rpc_url is required")
	}
	if v, ok := m["reconnection_interval"]; ok {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}

		if i <= 0 {
			return errors.New("reconnection_interval must be greater than 0")
		}

		c.ReconnectionInterval = i
	} else {
		c.ReconnectionInterval = 60
	}

	if v, ok := m["max_retries"]; ok {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}

		if i <= 0 {
			return errors.New("max_retries must be greater than 0")
		}

		c.MaxRetries = i
	} else {
		c.MaxRetries = 10
	}

	if v, ok := m["block_sync_chunk_size"]; ok {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}

		if i <= 0 {
			return errors.New("block_sync_chunk_size must be greater than 0")
		}

		c.BlockSyncChunkSize = i
	} else {
		c.BlockSyncChunkSize = 1000000
	}
	return nil
}

var (
	// lastSeenHeightKey is the key used to store the last height processed by the listener
	lastSeenHeightKey = []byte("lh")
	// lastHeightWithEventKey is the key used to store the last height with an event processed by the listener
	lastHeightWithEventKey = []byte("lwe")
)

// getLastSeenHeight gets the last height seen on the Ethereum blockchain.
func getLastSeenHeight(ctx context.Context, eventStore listeners.EventStore) (int64, error) {
	// get the last confirmed block height processed by the listener
	lastHeight, err := eventStore.Get(ctx, lastSeenHeightKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get last seed block height: %w", err)
	}

	if len(lastHeight) == 0 {
		return 0, nil
	}

	return int64(binary.LittleEndian.Uint64(lastHeight)), nil
}

// setLastSeenHeight sets the last height seen on the Ethereum blockchain.
func setLastSeenHeight(ctx context.Context, eventStore listeners.EventStore, height int64) error {
	heightBts := make([]byte, 8)
	binary.LittleEndian.PutUint64(heightBts, uint64(height))

	// set the last confirmed block height processed by the listener
	err := eventStore.Set(ctx, lastSeenHeightKey, heightBts)
	if err != nil {
		return fmt.Errorf("failed to set last seed block height: %w", err)
	}
	return nil
}

// getLastHeightWithEvent gets the last height that was processed by the listener and has an event.
// If this is the first one, it returns -1
func getLastHeightWithEvent(ctx context.Context, eventStore listeners.EventStore) (int64, error) {
	lastHeight, err := eventStore.Get(ctx, lastHeightWithEventKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get last used block height: %w", err)
	}

	if len(lastHeight) == 0 {
		return -1, nil
	}

	return int64(binary.LittleEndian.Uint64(lastHeight)), nil
}

// setLastHeightWithEvent sets the last height that was processed by the listener and has an event.
func setLastHeightWithEvent(ctx context.Context, eventStore listeners.EventStore, height int64) error {
	heightBts := make([]byte, 8)
	binary.LittleEndian.PutUint64(heightBts, uint64(height))

	err := eventStore.Set(ctx, lastHeightWithEventKey, heightBts)
	if err != nil {
		return fmt.Errorf("failed to set last used block height: %w", err)
	}

	return nil
}

// processEvents processes all received events for a given height range.
func (e EthListener) processEvents(ctx context.Context, from, to int64, client *ethClient, eventStore listeners.EventStore, logger *log.SugaredLogger) error {
	logs, err := client.GetEventLogs(ctx, from, to)
	if err != nil {
		return err
	}

	blocks := make(map[uint64][]logCopy)
	blockOrder := make([]uint64, 0, len(blocks))
	for _, log := range logs {
		if len(log.Topics) == 0 {
			logger.Debug("skipping event with no topics", "tx_hash", log.TxHash, "block_number", log.BlockNumber, "log_index", log.Index)
			continue
		}
		l2 := logCopy{
			Address:     log.Address,
			Topics:      log.Topics,
			Data:        log.Data,
			BlockNumber: log.BlockNumber,
			TxHash:      log.TxHash,
			TxIndex:     log.TxIndex,
			BlockHash:   log.BlockHash,
			Index:       log.Index,
			Removed:     log.Removed,
		}

		_, ok := blocks[log.BlockNumber]
		blocks[log.BlockNumber] = append(blocks[log.BlockNumber], l2)
		if !ok {
			blockOrder = append(blockOrder, log.BlockNumber)
		}
	}

	lastUsed, err := getLastHeightWithEvent(ctx, eventStore)
	if err != nil {
		return err
	}

	var lastUsedHeight *uint64
	if lastUsed != -1 {
		lastUsedHeight2 := uint64(lastUsed)
		lastUsedHeight = &lastUsedHeight2
	}

	for _, block := range blockOrder {
		logs := blocks[block]

		// ensure logs are sorted by index
		sort.Slice(logs, func(i, j int) bool {
			return logs[i].Index < logs[j].Index
		})

		bts, err := serialize.Encode(&blockData{
			Previous: lastUsedHeight,
			Height:   block,
			Logs:     logs,
		})
		if err != nil {
			return err
		}

		err = eventStore.Broadcast(ctx, e.resolutionExtName(), bts)
		if err != nil {
			return err
		}

		b2 := block // avoid closure issues
		lastUsedHeight = &b2
	}

	// it is important that this is set before the last seen height is set, since it is ok
	// to resubmit the same event, but not okay to submit an event with an invalid last height
	if len(blockOrder) > 0 {
		err = setLastHeightWithEvent(ctx, eventStore, int64(blockOrder[len(blockOrder)-1]))
		if err != nil {
			return err
		}
	}

	logger.Info("processed events", "from", from, "to", to, "event_count", len(logs))

	return setLastSeenHeight(ctx, eventStore, to)
}

// bestHeight chooses a height to start from. It chooses the heighest of all the heights.
// TODO: I think we can remove this. It was previously used when more than 2 potential best
// heights were passed in, but now we only pass in 2.
func bestHeight(option ...int64) int64 {
	var best int64
	for _, o := range option {
		if o > best {
			best = o
		}
	}
	return best
}

// resolutionConfig returns the resolution configuration for the EthListener.
func (e EthListener) resolutionConfig() resolutions.ResolutionConfig {
	return resolutions.ResolutionConfig{
		RefundThreshold:       e.RefundThreshold,
		ConfirmationThreshold: e.ConfirmationThreshold,
		ExpirationPeriod:      e.ExpirationPeriod,
		ResolveFunc: func(ctx context.Context, app *common.App, resolution *resolutions.Resolution, kwilBlock *common.BlockContext) error {
			block := blockData{}
			err := serialize.Decode(resolution.Body, &block)
			if err != nil {
				return fmt.Errorf("failed to decode block: %w", err)
			}

			var previous int64
			if block.Previous == nil {
				previous = -1
			} else {
				previous = int64(*block.Previous)
			}

			app.Service.Logger.Info("resolving block", "height", block.Height, "previous", previous, "log_count", len(block.Logs))

			_, err = e.tempStorageProc(ctx, kwilBlock, app, "store", []interface{}{block.Height, previous, resolution.Body})
			if err != nil {
				return fmt.Errorf("failed to store block: %w", err)
			}

			return nil
		},
	}
}

// Log represents a contract log event. These events are generated by the LOG opcode and
// stored/indexed by the node.
// It is a direct copy of go-ethereum's Log struct, except without RLP tags.
// TODO: we should use our own custom serialization which would be more efficient
// and allow us to remove this struct.
type logCopy struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address ethcommon.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []ethcommon.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data []byte `json:"data" gencodec:"required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash ethcommon.Hash `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex uint `json:"transactionIndex"`
	// hash of the block in which the transaction was included
	BlockHash ethcommon.Hash `json:"blockHash"`
	// index of the log in the block
	Index uint `json:"logIndex"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}

// blockData is the data for a block.
type blockData struct {
	// The current block height.
	Height uint64
	// The logs for the block.
	Logs []logCopy
	// The previous block height.
	Previous *uint64 `rlp:"optional"`
}
