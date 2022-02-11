package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"io/ioutil"
	"net/http"
)

type TipSetMessage struct {
	Height       int64    `json:"height"`
	Timestamp    int64    `json:"timestamp"`
	MessageCount int64    `json:"messageCount"`
	Blocks       []Blocks `json:"blocks"`
}

type Blocks struct {
	Cid string `json:"cid"`
}

func GetSectorTicketOnChain(ctx context.Context, fullNodeApi v0api.FullNode, maddr address.Address, ts *types.TipSet, preCommitInfo *miner.SectorPreCommitOnChainInfo) (abi.Randomness, error) {
	buf := new(bytes.Buffer)
	if err := maddr.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("Address MarshalCBOR err:", err)
	}

	ticket, err := fullNodeApi.StateGetRandomnessFromTickets(ctx, crypto.DomainSeparationTag_SealRandomness, preCommitInfo.Info.SealRandEpoch, buf.Bytes(), ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("Getting Randomness err:", err)
	}

	return ticket, err
}

func GetSectorCommitInfoOnChain(ctx context.Context, fullNodeApi v0api.FullNode, maddr address.Address, sid abi.SectorNumber) (*types.TipSet, *miner.SectorPreCommitOnChainInfo, error) {
	si, err := fullNodeApi.StateSectorGetInfo(ctx, maddr, sid, types.EmptyTSK)
	if err != nil {
		return nil, nil, err
	}

	if si == nil {
		//Provecommit not submitted
		preCommitInfo, err := fullNodeApi.StateSectorPreCommitInfo(ctx, maddr, sid, types.EmptyTSK)
		if err != nil {
			return nil, nil, xerrors.Errorf("Getting sector PreCommit info err: %w", err)
		}

		ts, err := fullNodeApi.ChainGetTipSetByHeight(ctx, preCommitInfo.PreCommitEpoch, types.EmptyTSK)
		if err != nil {
			return nil, nil, err
		}
		if ts == nil {
			return nil, nil, xerrors.Errorf("Height(%d) Tipset Not Found")
		}
		return ts, &preCommitInfo, err
	}

	h := new(http.Client)
	url := fmt.Sprintf("https://filfox.info/api/v1/tipset/%d", si.Activation)
	resp, err := h.Get(url)
	if err != nil {
		return nil, nil, xerrors.Errorf("get block id err: %w", err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	tipSetMessage := new(TipSetMessage)
	json.Unmarshal(body, &tipSetMessage)

	tipSet := types.EmptyTSK
	if len(tipSetMessage.Blocks) > 0 {
		c, _ := cid.Decode(tipSetMessage.Blocks[0].Cid)
		cids := make([]cid.Cid, 1)
		cids[0] = c
		tipSet = types.NewTipSetKey(cids...)
	}
	//fmt.Println(tipSet)

	//ts, err := fullNodeApi.ChainGetTipSetByHeight(ctx, si.Activation, types.EmptyTSK)
	ts, err := fullNodeApi.ChainGetTipSetByHeight(ctx, si.Activation, tipSet)
	if err != nil {
		return nil, nil, err
	}
	if ts == nil {
		return nil, nil, xerrors.Errorf("Height(%d) Tipset Not Found", si.Activation)
	}

	preCommitInfo, err := fullNodeApi.StateSectorPreCommitInfo(ctx, maddr, sid, ts.Key())
	if err != nil {
		return nil, nil, xerrors.Errorf("Getting sector PreCommit info err:", err)
	}

	return ts, &preCommitInfo, err
}
