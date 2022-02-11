package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/roc-daniel/filecoin-sealer-recover/export"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var log = logging.Logger("recover")
var db *leveldb.Datastore

type PreCommitParam struct {
	Sb            *ffiwrapper.Sealer
	Sid           storage.SectorRef
	Phase1Out     storage.PreCommit1Out
	SealedCID     string
	TempDir       string
	SealingResult string
}

var paramChannel chan *PreCommitParam

var RecoverCmd = &cli.Command{
	Name:      "recover",
	Usage:     "Recovery sector tools",
	ArgsUsage: "[sectorNum1 sectorNum2 ...]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "sectors-recovery-metadata",
			Aliases:  []string{"metadata"},
			Usage:    "specify the metadata file for the sectors recovery (Exported json file)",
			Required: true,
		},
		&cli.UintFlag{
			Name:  "parallel",
			Usage: "Number of parallel P1",
			Value: 1,
		},
		&cli.StringFlag{
			Name:  "sealing-result",
			Value: "./sector",
			Usage: "Recover sector result path",
		},
		&cli.StringFlag{
			Name:  "sealing-temp",
			Value: "./temp",
			Usage: "Temporarily generated during sector file",
		},
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Start sealer recovery!")

		ctx := cliutil.DaemonContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		//if cctx.Args().Len() < 1 {
		//	return fmt.Errorf("at least one sector must be specified")
		//}

		pssb := cctx.String("sectors-recovery-metadata")
		if pssb == "" {
			return xerrors.Errorf("Undefined sectors recovery metadata")
		}

		log.Infof("Importing sectors recovery metadata for %s", pssb)

		rp, err := migrateRecoverMeta(ctx, pssb)
		if err != nil {
			return xerrors.Errorf("migrating sectors metadata: %w", err)
		}

		//cmdSectors := make([]uint64, 0)
		//for _, sn := range cctx.Args().Slice() {
		//	sectorNum, err := strconv.ParseUint(sn, 10, 64)
		//	if err != nil {
		//		return fmt.Errorf("could not parse sector number: %w", err)
		//	}
		//	cmdSectors = append(cmdSectors, sectorNum)
		//}

		fs, err := homedir.Expand("data")
		if err != nil {
			return xerrors.Errorf("get homedir: %v", err)
		}
		db, err = leveldb.NewDatastore(fs, nil)
		if err != nil {
			return xerrors.Errorf("get data: %v", err)
		}
		defer db.Close()

		skipSectors := make([]uint64, 0)
		runSectors := make([]uint64, 0)
		sectorInfos := make(export.SectorInfos, 0)

		for _, sectorInfo := range rp.SectorInfos {
			key := fmt.Sprintf("pc2-%s", sectorInfo.SectorNumber.String())
			b, err := db.Has(datastore.NewKey(key))
			if err != nil {
				return xerrors.Errorf("get key: %s, %v", key, err)
			}

			if !b {
				sectorInfos = append(sectorInfos, sectorInfo)
				runSectors = append(runSectors, uint64(sectorInfo.SectorNumber))
			} else {
				skipSectors = append(skipSectors, uint64(sectorInfo.SectorNumber))
			}
		}

		//for _, sn := range cmdSectors {
		//	run := false
		//	for _, sectorInfo := range rp.SectorInfos {
		//		if sn == uint64(sectorInfo.SectorNumber) {
		//			run = true
		//			sectorInfos = append(sectorInfos, sectorInfo)
		//			runSectors = append(runSectors, sn)
		//		}
		//	}
		//	if !run {
		//		skipSectors = append(skipSectors, sn)
		//	}
		//}

		if len(runSectors) > 0 {
			log.Infof("Sector %v to be recovered, %d in total!", runSectors, len(runSectors))
		}
		if len(skipSectors) > 0 {
			log.Warnf("Skip sector %v, %d in total, because sector has compeleted sealed!", skipSectors, len(skipSectors))
		}

		rp.SectorInfos = sectorInfos

		paramChannel = make(chan *PreCommitParam, 13)
		go HandlePreCommit2()

		if err = RecoverSealedFile(ctx, rp, cctx.Uint("parallel"), cctx.String("sealing-result"), cctx.String("sealing-temp")); err != nil {
			return err
		}

		log.Info("Complete recovery sealed， waiting pc2")

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sigs
		close(paramChannel)

		log.Info("Complete pc2 sealed")

		return nil
	},
}

func migrateRecoverMeta(ctx context.Context, metadata string) (export.RecoveryParams, error) {
	metadata, err := homedir.Expand(metadata)
	if err != nil {
		return export.RecoveryParams{}, xerrors.Errorf("expanding sectors recovery dir: %w", err)
	}

	b, err := ioutil.ReadFile(metadata)
	if err != nil {
		return export.RecoveryParams{}, xerrors.Errorf("reading sectors recovery metadata: %w", err)
	}

	rp := export.RecoveryParams{}
	if err := json.Unmarshal(b, &rp); err != nil {
		return export.RecoveryParams{}, xerrors.Errorf("unmarshalling sectors recovery metadata: %w", err)
	}

	return rp, nil
}

func RecoverSealedFile(ctx context.Context, rp export.RecoveryParams, parallel uint, sealingResult string, sealingTemp string) error {
	actorID, err := address.IDFromAddress(rp.Miner)
	if err != nil {
		return xerrors.Errorf("Getting IDFromAddress err:", err)
	}

	wg := &sync.WaitGroup{}
	limiter := make(chan bool, parallel)
	var p1LastTaskTime time.Time

	for _, sector := range rp.SectorInfos {
		key := fmt.Sprintf("pc1-%s", sector.SectorNumber.String())
		b, err := db.Has(datastore.NewKey(key))
		if err != nil {
			log.Errorf("Sector (%d) , has pc1o error: %v", sector.SectorNumber, err)
		}

		wg.Add(1)
		limiter <- true

		go func(sector *export.SectorInfo, b bool) {
			defer func() {
				wg.Done()
				<-limiter
			}()

			//Control PC1 running interval
			for {
				if time.Now().Add(-time.Minute * 3).After(p1LastTaskTime) {
					break
				}
				<-time.After(p1LastTaskTime.Sub(time.Now()))
			}
			p1LastTaskTime = time.Now()

			sdir, err := homedir.Expand(sealingTemp)
			if err != nil {
				log.Errorf("Sector (%d) ,expands the path error: %v", sector.SectorNumber, err)
				return
			}
			mkdirAll(sdir)
			tempDir, err := ioutil.TempDir(sdir, fmt.Sprintf("recover-%d", sector.SectorNumber))
			if err != nil {
				log.Errorf("Sector (%d) ,creates a new temporary directory error: %v", sector.SectorNumber, err)
				return
			}
			if err := os.MkdirAll(tempDir, 0775); err != nil {
				log.Errorf("Sector (%d) ,creates a directory named path error: %v", sector.SectorNumber, err)
				return
			}
			sb, err := ffiwrapper.New(&basicfs.Provider{
				Root: tempDir,
			})
			if err != nil {
				log.Errorf("Sector (%d) ,new ffi Sealer error: %v", sector.SectorNumber, err)
				return
			}

			sid := storage.SectorRef{
				ID: abi.SectorID{
					Miner:  abi.ActorID(actorID),
					Number: sector.SectorNumber,
				},
				ProofType: sector.SealProof,
			}

			log.Infof("Start recover sector(%d,%d), registeredSealProof: %d, ticket: %x", actorID, sector.SectorNumber, sector.SealProof, sector.Ticket)

			var pc1o storage.PreCommit1Out
			if !b {
				log.Infof("Start running AP, sector (%d)", sector.SectorNumber)

				pi, err := sb.AddPiece(context.TODO(), sid, nil, abi.PaddedPieceSize(rp.SectorSize).Unpadded(), sealing.NewNullReader(abi.UnpaddedPieceSize(rp.SectorSize)))
				if err != nil {
					log.Errorf("Sector (%d) ,running AP  error: %v", sector.SectorNumber, err)
					return
				}
				var pieces []abi.PieceInfo
				pieces = append(pieces, pi)

				log.Infof("Complete AP, sector (%d)", sector.SectorNumber)

				log.Infof("Start running PreCommit1, sector (%d)", sector.SectorNumber)

				pc1o, err = sb.SealPreCommit1(context.TODO(), sid, abi.SealRandomness(sector.Ticket), []abi.PieceInfo{pi})
				if err != nil {
					log.Errorf("Sector (%d) , running PreCommit1  error: %v", sector.SectorNumber, err)
					return
				}
				//time.Sleep(time.Minute * 5)

				if pc1o != nil && len(pc1o) > 0 {
					log.Infof("Complete PreCommit1, sector (%d)", sector.SectorNumber)

					key := fmt.Sprintf("pc1-%s", sector.SectorNumber.String())
					err = db.Put(datastore.NewKey(key), pc1o)
					if err != nil {
						log.Errorf("Sector (%d) , put pc1o error: %v", sector.SectorNumber, err)
					}
				}
			} else {
				key := fmt.Sprintf("pc1-%s", sector.SectorNumber.String())
				pc1o, err = db.Get(datastore.NewKey(key))
				if err != nil {
					log.Errorf("Sector (%d) , get pc1o error: %v", sector.SectorNumber, err)
					return
				}
				log.Infof("Have Completed PreCommit1, sector (%d)", sector.SectorNumber)
			}

			p := &PreCommitParam{
				Sb:            sb,
				Sid:           sid,
				Phase1Out:     pc1o,
				SealedCID:     sector.SealedCID.String(),
				TempDir:       tempDir,
				SealingResult: sealingResult,
			}
			paramChannel <- p

			//err = sealPreCommit2AndCheck(ctx, sb, sid, pc1o, sector.SealedCID.String())
			//if err != nil {
			//	log.Errorf("Sector (%d) , running PreCommit2  error: %v", sector.SectorNumber, err)
			//}
			//
			//err = MoveStorage(ctx, sid, tempDir, sealingResult)
			//if err != nil {
			//	log.Errorf("Sector (%d) , running MoveStorage  error: %v", sector.SectorNumber, err)
			//}
			//
			//key := fmt.Sprintf("pc2-%s", sector.SectorNumber.String())
			//err = db.Put(datastore.NewKey(key), []byte("success"))
			//if err != nil {
			//	log.Errorf("Sector (%d) , put pc2 error: %v", sector.SectorNumber, err)
			//}
			//
			//log.Infof("Complete sector (%d)", sector.SectorNumber)
		}(sector, b)
	}
	wg.Wait()

	return nil
}

func HandlePreCommit2() {
	for {
		select {
		case p := <-paramChannel:
			number := p.Sid.ID.Number

			log.Infof("Start running PreCommit2, sector (%d)", number)
			ctx := context.Background()

			err := sealPreCommit2AndCheck(ctx, p.Sb, p.Sid, p.Phase1Out, p.SealedCID)
			if err != nil {
				log.Errorf("Sector (%d) , running PreCommit2  error: %v", number, err)
				continue
			}

			err = MoveStorage(ctx, p.Sid, p.TempDir, p.SealingResult)
			if err != nil {
				log.Errorf("Sector (%d) , running MoveStorage  error: %v", number, err)
				continue
			}

			//time.Sleep(time.Minute)

			key := fmt.Sprintf("pc2-%s", number.String())
			err = db.Put(datastore.NewKey(key), []byte("success"))
			if err != nil {
				log.Errorf("Sector (%d) , put pc2 error: %v", number, err)
			}

			log.Infof("Complete PreCommit2, sector (%d)", number)
		}
	}
}

//var pc2Lock sync.Mutex

func sealPreCommit2AndCheck(ctx context.Context, sb *ffiwrapper.Sealer, sid storage.SectorRef, phase1Out storage.PreCommit1Out, sealedCID string) error {
	//pc2Lock.Lock()
	cids, err := sb.SealPreCommit2(ctx, sid, phase1Out)
	if err != nil {
		//pc2Lock.Unlock()
		return err
	}

	if cids.Sealed.String() == "" {
		return xerrors.Errorf("sealed cid %s", cids.Sealed.String())
	}

	//pc2Lock.Unlock()
	log.Infof("Complete PreCommit2, sector (%d)", sid.ID)

	//check CID with chain
	if sealedCID != cids.Sealed.String() {
		return xerrors.Errorf("sealed cid mismatching!!! (sealedCID: %v, newSealedCID: %v)", sealedCID, cids.Sealed.String())
	}
	return nil
}

func MoveStorage(ctx context.Context, sector storage.SectorRef, tempDir string, sealingResult string) error {
	//del unseal
	if err := os.RemoveAll(tempDir + "/unsealed"); err != nil {
		return xerrors.Errorf("SectorID: %d, del unseal error：%s", sector.ID, err)
	}
	sectorNum := "s-t0" + sector.ID.Miner.String() + "-" + sector.ID.Number.String()

	//del layer
	files, _ := ioutil.ReadDir(tempDir + "/cache/" + sectorNum)
	for _, f := range files {
		if strings.Contains(f.Name(), "layer") || strings.Contains(f.Name(), "tree-c") || strings.Contains(f.Name(), "tree-d") {
			if err := os.RemoveAll(tempDir + "/cache/" + sectorNum + "/" + f.Name()); err != nil {
				return xerrors.Errorf("SectorID: %d, del layer error：%s", sector.ID, err)
			}
		}
	}

	//move to storage
	mkdirAll(sealingResult)
	mkdirAll(sealingResult + "/cache")
	mkdirAll(sealingResult + "/sealed")
	if err := move(tempDir+"/cache/"+sectorNum, sealingResult+"/cache/"+sectorNum); err != nil {
		// return xerrors.Errorf("SectorID: %d, move cache error：%s", sector.ID, err)
		// change the output to warn info since this will no impact the result
		log.Warn("can move sector to your sealingResult, reason: ", err)
		return nil
	}
	if err := move(tempDir+"/sealed/"+sectorNum, sealingResult+"/sealed/"+sectorNum); err != nil {
		return xerrors.Errorf("SectorID: %d, move sealed error：%s", sector.ID, err)
	}

	return nil
}
