package eth_oracle

import (
	"context"
	_ "embed"

	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/types/serialize"
	"github.com/kwilteam/kwil-db/extensions/hooks"
	"github.com/kwilteam/kwil-db/parse"
)

var (
	//go:embed temp_storage.kf
	tempStorageSchemaBytes []byte
	tempStorageSchema      *types.Schema
)

func init() {
	// parse the schema
	var err error
	tempStorageSchema, err = parse.Parse(tempStorageSchemaBytes)
	if err != nil {
		panic(err)
	}
}

// returns a genesis hook that deploys the temp_storage schema, which is used
// for ordering incoming events
func (e EthListener) genesisHook() hooks.GenesisHook {
	return func(ctx context.Context, app *common.App, chain *common.ChainContext) error {
		kwilBlock := &common.BlockContext{
			ChainContext: chain,
			Height:       0,
		}

		err := app.Engine.CreateDataset(&common.TxContext{
			Ctx:           ctx,
			BlockContext:  kwilBlock,
			TxID:          e.uniqueName("temp_storage_schema"),
			Signer:        []byte(e.ExtensionName),
			Caller:        e.ExtensionName,
			Authenticator: "eth_listener_extension",
		}, app.DB, tempStorageSchema)
		if err != nil {
			return err
		}

		// need to insert the original data into the temp storage
		_, err = e.tempStorageProc(ctx, kwilBlock, app, "init", nil)
		return err
	}
}

// uniqueName returns a unique namespaced string, which is useful for ensuring txid uniqueness
func (e EthListener) uniqueName(name string) string {
	return e.ExtensionName + "." + name
}

// endBlockHook returns a hook that is run at the end of every block.
// It is used to process the events that were stored in the temp storage
func (e EthListener) endBlockHook() hooks.EndBlockHook {
	return func(ctx context.Context, app *common.App, kwilBlock *common.BlockContext) error {
		processed, err := e.tempStorageProc(ctx, kwilBlock, app, "get_and_delete_ready", nil)
		if err != nil {
			return err
		}

		for _, row := range processed.Rows {
			// first column is height, second is the rlp encoded log.
			block := blockData{
				Height: uint64(row[0].(int64)),
			}
			if err := serialize.Decode(row[1].([]byte), &block); err != nil {
				return err
			}

			for _, log := range block.Logs {
				err = e.Resolve(ctx, app, ethTypes.Log{
					Address:     log.Address,
					Topics:      log.Topics,
					Data:        log.Data,
					BlockNumber: log.BlockNumber,
					TxHash:      log.TxHash,
					TxIndex:     log.TxIndex,
					BlockHash:   log.BlockHash,
					Index:       log.Index,
					Removed:     log.Removed,
				})
				if err != nil {
					return err
				}
			}
		}

		return nil
	}
}

// tempStorageProc calls a procedure on the temp storage dataset
func (e EthListener) tempStorageProc(ctx context.Context, block *common.BlockContext, app *common.App, procedure string, args []any) (*sql.ResultSet, error) {
	return app.Engine.Procedure(&common.TxContext{
		Ctx:          ctx,
		BlockContext: block,
		TxID:         e.uniqueName(procedure),
		Signer:       []byte(e.ExtensionName),
		Caller:       e.ExtensionName,
	}, app.DB, &common.ExecutionData{
		Dataset:   tempStorageSchema.DBID(),
		Procedure: procedure,
		Args:      args,
	})
}
