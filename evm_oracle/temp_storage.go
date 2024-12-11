package eth_oracle

import (
	"context"
	_ "embed"
	"fmt"

	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/types/serialize"
	"github.com/kwilteam/kwil-db/core/utils"
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
		rows, err := e.tempStorageProc(ctx, kwilBlock, app, "get_and_delete_ready", nil)
		if err != nil {
			return err
		}

		var processed []*tempStorageRes
		for _, row := range rows.Rows {
			processed = append(processed, &tempStorageRes{
				Height: row[0].(int64),
				Data:   row[1].([]byte),
			})
		}

		for _, row := range processed {
			// first column is height, second is the rlp encoded log.
			block := blockData{
				Height: uint64(row.Height),
			}
			if err := serialize.Decode(row.Data, &block); err != nil {
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
				}, kwilBlock)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}
}

type tempStorageRes struct {
	Height int64
	Data   []byte
}

// getAndDeleteReady calls the get_and_delete_ready procedure on the temp storage dataset
func (e EthListener) getAndDeleteReady(ctx context.Context, block *common.BlockContext, app *common.App) ([]*tempStorageRes, error) {
	q := querier{
		e:   &e,
		c:   app,
		b:   block,
		ctx: ctx,
	}

	res, err := q.Query("SELECT height FROM last_processed", nil)
	if err != nil {
		return nil, err
	}

	if len(res.Rows) != 1 {
		return nil, fmt.Errorf("expected 1 row, got %d", len(res.Rows))
	}

	height := res.Rows[0][0].(int64)

	res, err = q.Query("SELECT height, previous_height, data FROM data WHERE height > $last_processed_height ORDER BY height ASC", map[string]any{
		"$last_processed_height": height,
	})
	if err != nil {
		return nil, err
	}

	if len(res.Rows) == 0 {
		return nil, nil
	}

	var results []*tempStorageRes

	for _, row := range res.Rows {
		if row[1].(int64) != height {
			_, err = q.Query("UPDATE last_processed SET height = $new_height", map[string]any{
				"$new_height": row[1],
			})
			if err != nil {
				return nil, err
			}
			break
		}

		_, err = q.Query("DELETE FROM data WHERE height = $height", map[string]any{
			"$height": row[0],
		})
		if err != nil {
			return nil, err
		}

		height = row[0].(int64)
		results = append(results, &tempStorageRes{
			Height: row[0].(int64),
			Data:   row[2].([]byte),
		})
	}

	_, err = q.Query("UPDATE last_processed SET height = $new_height", map[string]any{
		"$new_height": height,
	})
	if err != nil {
		return nil, err
	}

	return results, err
}

type querier struct {
	e   *EthListener
	c   *common.App
	b   *common.BlockContext
	ctx context.Context
}

func (q querier) Query(query string, vals map[string]any) (*sql.ResultSet, error) {
	return q.c.Engine.Execute(&common.TxContext{
		Ctx:          q.ctx,
		BlockContext: q.b,
		TxID:         q.e.uniqueName("a"),
		Signer:       []byte(q.e.ExtensionName),
		Caller:       q.e.ExtensionName,
	}, q.c.DB, utils.GenerateDBID(tempStorageSchema.Name, []byte(q.e.ExtensionName)), query, vals)
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
		Dataset:   utils.GenerateDBID(tempStorageSchema.Name, []byte(e.ExtensionName)),
		Procedure: procedure,
		Args:      args,
	})
}
