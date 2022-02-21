package recovery

import (
	"bufio"
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
	"github.com/go-redis/redis/v8"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/roc-daniel/filecoin-sealer-recover/export"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

var log = logging.Logger("recover")

//var db *leveldb.Datastore
var errorSectorFile *os.File
var redisDB *redis.Client
var closing bool
var done chan bool

const PC1_RESULT_KEY = "pc1-result"
const PC2_RESULT_KEY = "pc2-result"
const PC2_COMPUTING_TMEP_KEY = "pc2-computing-temp"
const QUEUE_KEY = "queue"

type PreCommitParam struct {
	SectorInfo export.SectorInfo
	Phase1Out  []byte
}

//var paramChannel chan *PreCommitParam

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
		&cli.BoolFlag{
			Name:  "precommit1",
			Value: true,
			Usage: "enable precommit1 (32G sectors: 1 core, 128GiB Memory)",
		},
		&cli.BoolFlag{
			Name:  "precommit2",
			Value: true,
			Usage: "enable precommit2 (32G sectors: all cores, 96GiB Memory)",
		},
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Start sealer recovery!")

		ctx := cliutil.DaemonContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		pssb := cctx.String("sectors-recovery-metadata")
		if pssb == "" {
			return xerrors.Errorf("Undefined sectors recovery metadata")
		}

		log.Infof("Importing sectors recovery metadata for %s", pssb)

		rp, err := migrateRecoverMeta(ctx, pssb)
		if err != nil {
			return xerrors.Errorf("migrating sectors metadata: %w", err)
		}

		redisDB = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   0,
		})
		defer redisDB.Close()

		cmd := redisDB.Ping(ctx)
		if cmd.Err() != nil {
			return xerrors.Errorf("connect redis: %v", cmd.Err())
		}

		done = make(chan bool, 1)

		if cctx.Bool("precommit1") {
			skipSectors := make([]uint64, 0)
			runSectors := make([]uint64, 0)
			sectorInfos := make(export.SectorInfos, 0)

			for _, sectorInfo := range rp.SectorInfos {
				b, err := redisDB.HExists(ctx, PC2_RESULT_KEY, sectorInfo.SectorNumber.String()).Result()
				if err != nil && err != redis.Nil {
					return xerrors.Errorf("get key: %s, %v", sectorInfo.SectorNumber.String(), err)
				}

				if !b {
					sectorInfos = append(sectorInfos, sectorInfo)
					runSectors = append(runSectors, uint64(sectorInfo.SectorNumber))
				} else {
					skipSectors = append(skipSectors, uint64(sectorInfo.SectorNumber))
				}
			}

			if len(runSectors) > 0 {
				log.Infof("Sector %v to be recovered, %d in total!", runSectors, len(runSectors))
			}
			if len(skipSectors) > 0 {
				log.Warnf("Skip sector %v, %d in total, because sector has compeleted sealed!", skipSectors, len(skipSectors))
			}

			rp.SectorInfos = sectorInfos

			go RecoverPC1SealedFile(ctx, rp, cctx.Uint("parallel"), cctx.String("sealing-temp"))

		} else if cctx.Bool("precommit2") {
			errorSector, err := homedir.Expand("error-sector.txt")
			if err != nil {
				return xerrors.Errorf("get homedir: %v", err)
			}

			errorSectorFile, err = os.OpenFile(errorSector, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				cancel()
				return xerrors.Errorf("open file: %v", err)
			}
			defer errorSectorFile.Close()

			go RecoverPC2SealedFile(ctx, rp, cctx.String("sealing-result"), cctx.String("sealing-temp"))
		}

		log.Info("Sealer recovery has started")

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sigs

		closing = true
		<-done

		log.Info("Sealer recovery finished")

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

func RecoverPC1SealedFile(ctx context.Context, rp export.RecoveryParams, parallel uint, sealingTemp string) {
	actorID, err := address.IDFromAddress(rp.Miner)
	if err != nil {
		log.Panicf("Getting IDFromAddress err:", err)
	}

	wg := &sync.WaitGroup{}
	limiter := make(chan bool, parallel)
	var p1LastTaskTime time.Time

	for _, sector := range rp.SectorInfos {
		if closing {
			break
		}

		b, err := redisDB.HExists(ctx, PC1_RESULT_KEY, sector.SectorNumber.String()).Result()
		if err != nil && err != redis.Nil {
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
			//tempDir, err := ioutil.TempDir(sdir, fmt.Sprintf("recover-%d", sector.SectorNumber))
			tempDir := path.Join(sdir, fmt.Sprintf("recover-%d", sector.SectorNumber))
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

				pi, err := sb.AddPiece(ctx, sid, nil, abi.PaddedPieceSize(rp.SectorSize).Unpadded(), sealing.NewNullReader(abi.UnpaddedPieceSize(rp.SectorSize)))
				if err != nil {
					log.Errorf("Sector (%d) ,running AP  error: %v", sector.SectorNumber, err)
					return
				}
				var pieces []abi.PieceInfo
				pieces = append(pieces, pi)

				log.Infof("Complete AP, sector (%d)", sector.SectorNumber)

				log.Infof("Start running PreCommit1, sector (%d)", sector.SectorNumber)

				pc1o, err = sb.SealPreCommit1(ctx, sid, abi.SealRandomness(sector.Ticket), []abi.PieceInfo{pi})
				if err != nil {
					log.Errorf("Sector (%d) , running PreCommit1  error: %v", sector.SectorNumber, err)
					return
				}
				if pc1o == nil || len(pc1o) <= 0 {
					//restart
					return
				}

				log.Infof("Complete PreCommit1, sector (%d)", sector.SectorNumber)

				_, err = redisDB.HSet(ctx, PC1_RESULT_KEY, sector.SectorNumber.String(), []byte(pc1o)).Result()
				if err != nil {
					log.Errorf("Sector (%d) , put pc1o error: %v", sector.SectorNumber, err)
				}
			} else {
				pc1oStr, err := redisDB.HGet(ctx, PC1_RESULT_KEY, sector.SectorNumber.String()).Result()
				if err != nil {
					log.Errorf("Sector (%d) , get pc1o error: %v", sector.SectorNumber, err)
					return
				}
				pc1o = []byte(pc1oStr)
				log.Infof("Have Completed PreCommit1, sector (%d)", sector.SectorNumber)
			}

			p := &PreCommitParam{
				SectorInfo: *sector,
				Phase1Out:  pc1o,
			}
			pStr, _ := json.Marshal(p)
			_, err = redisDB.LPush(ctx, QUEUE_KEY, pStr).Result()
			if err != nil {
				log.Errorf("Sector (%d) , lpush queue error: %v", sector.SectorNumber, err)
				return
			}

		}(sector, b)
	}
	wg.Wait()

	done <- true
}

func RecoverPC2SealedFile(ctx context.Context, rp export.RecoveryParams, sealingResult string, sealingTemp string) {
	actorID, err := address.IDFromAddress(rp.Miner)
	if err != nil {
		log.Panicf("Getting IDFromAddress err:", err)
	}

	//处理重启的情况
	content, err := redisDB.HGetAll(ctx, PC2_COMPUTING_TMEP_KEY).Result()
	if err != nil && err != redis.Nil {
		log.Panicf("Redis hgetall temp: %v", err)
	}
	for _, value := range content {
		HandlePreCommit2(ctx, actorID, sealingTemp, sealingResult, value)
	}

	//接收队列
	for {
		if closing {
			break
		}

		valueSlice, err := redisDB.BRPop(ctx, 5*time.Second, QUEUE_KEY).Result()
		if err == redis.Nil {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			log.Errorf("Redis brpop: %v", err)
			continue
		}

		for _, value := range valueSlice {
			HandlePreCommit2(ctx, actorID, sealingTemp, sealingResult, value)
		}
	}

	done <- true
}

func HandlePreCommit2(ctx context.Context, actorID uint64, sealingTemp, sealingResult string, value string) {
	var preCommitParam PreCommitParam
	json.Unmarshal([]byte(value), &preCommitParam)

	//记录正在处理的
	_, err := redisDB.HSet(ctx, PC2_COMPUTING_TMEP_KEY, preCommitParam.SectorInfo.SectorNumber.String(), value).Result()
	if err != nil {
		log.Errorf("Redis hset temp: %v", err)
	}

	HandleOnePreCommit2(ctx, actorID, &preCommitParam.SectorInfo, sealingTemp, sealingResult, preCommitParam.Phase1Out)

	//删除正在处理的
	_, err = redisDB.HDel(ctx, PC2_COMPUTING_TMEP_KEY, preCommitParam.SectorInfo.SectorNumber.String()).Result()
	if err != nil {
		log.Errorf("Redis hdel temp: %v", err)
	}
}

func HandleOnePreCommit2(ctx context.Context, actorID uint64, sector *export.SectorInfo, sealingTemp, sealingResult string, phase1Out storage.PreCommit1Out) {
	number := sector.SectorNumber
	log.Infof("Start running PreCommit2, sector (%d)", number)

	sdir, err := homedir.Expand(sealingTemp)
	if err != nil {
		log.Errorf("Sector (%d) ,expands the path error: %v", number, err)
		return
	}
	mkdirAll(sdir)
	//tempDir, err := ioutil.TempDir(sdir, fmt.Sprintf("recover-%d", sector.SectorNumber))
	tempDir := path.Join(sdir, fmt.Sprintf("recover-%d", number))
	if err != nil {
		log.Errorf("Sector (%d) ,creates a new temporary directory error: %v", number, err)
		return
	}
	if err := os.MkdirAll(tempDir, 0775); err != nil {
		log.Errorf("Sector (%d) ,creates a directory named path error: %v", number, err)
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

	cids, err := sb.SealPreCommit2(ctx, sid, phase1Out)
	if err != nil {
		log.Errorf("Sector (%d) , running PreCommit2  error: %v", number, err)
		_ = os.RemoveAll(tempDir)
		WriteErrorSector(number.String(), err.Error())
		return
	}

	if cids.Sealed.String() == "" {
		return
	}

	log.Infof("Complete PreCommit2, sector (%d)", number)

	//check CID with chain
	if sector.SealedCID.String() != cids.Sealed.String() {
		err := xerrors.Errorf("sealed cid mismatching!!! (sealedCID: %v, newSealedCID: %v)", sector.SealedCID.String(), cids.Sealed.String())
		log.Errorf("Sector (%d) , running PreCommit2  error: %v", number, err)
		_ = os.RemoveAll(tempDir)
		WriteErrorSector(number.String(), "mismatching")
		return
	}

	err = MoveStorage(ctx, sid, tempDir, sealingResult)
	if err != nil {
		log.Errorf("Sector (%d) , running MoveStorage  error: %v", number, err)
		return
	}

	_, err = redisDB.HSet(ctx, PC2_RESULT_KEY, number.String(), []byte("success")).Result()
	if err != nil {
		log.Errorf("Sector (%d) , put pc2 error: %v", number, err)
	}

	log.Infof("Complete PreCommit2, sector (%d)", number)
}

func WriteErrorSector(sectorId string, reason string) {
	write := bufio.NewWriter(errorSectorFile)
	write.WriteString(sectorId + "-" + reason + "\r\n")
	write.Flush()
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
