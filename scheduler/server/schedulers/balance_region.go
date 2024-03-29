// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// 获取 down 时间不能超过集群的 MaxStoreDownTime 的 store
	var suitableStores []*core.StoreInfo
	for _, s := range cluster.GetStores() {
		if s.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, s)
		}
	}

	// region 大小排序
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	// 根据 regionSize 找 region
	var region *core.RegionInfo = nil
	for i := 0; i < len(suitableStores) && region == nil; i++ {
		s := suitableStores[i]
		if i > 0 && suitableStores[i-1].GetRegionSize() == s.GetRegionSize() {
			continue
		}

		for j := i; j < len(suitableStores) && region == nil; j++ {
			ss := suitableStores[j]
			if s.GetRegionSize() != ss.GetRegionSize() {
				break
			}
			cluster.GetPendingRegionsWithLock(ss.GetID(), func(container core.RegionsContainer) {
				if r := container.RandomRegion([]byte{}, []byte{}); r != nil {
					region = r
				}
			})
		}

		for j := i; j < len(suitableStores) && region == nil; j++ {
			ss := suitableStores[j]
			if s.GetRegionSize() != ss.GetRegionSize() {
				break
			}
			cluster.GetFollowersWithLock(ss.GetID(), func(container core.RegionsContainer) {
				if r := container.RandomRegion([]byte{}, []byte{}); r != nil {
					region = r
				}
			})
		}

		for j := i; j < len(suitableStores) && region == nil; j++ {
			ss := suitableStores[j]
			if s.GetRegionSize() != ss.GetRegionSize() {
				break
			}
			cluster.GetLeadersWithLock(ss.GetID(), func(container core.RegionsContainer) {
				if r := container.RandomRegion([]byte{}, []byte{}); r != nil {
					region = r
				}
			})
		}
	}

	if region == nil || len(region.GetPeers()) < cluster.GetMaxReplicas() {
		return nil
	}

	// 寻找最小的 store
	var storeTo uint64
	var storeIs bool = false
	for i := len(suitableStores) - 1; i >= 0; i-- {
		// region 是否存在 peer 在 store 中
		sId := suitableStores[i].GetID()
		if suitableStores[i].IsOffline() {
			continue
		}
		if _, ok := region.GetStoreIds()[sId]; !ok {
			storeIs = true
			storeTo = sId
			break
		}
	}

	if !storeIs {
		return nil
	}

	// 从 region 中找最大的 store
	var storeForm uint64
	var storeFormSize int64
	for sId := range region.GetStoreIds() {
		if sId != storeTo && cluster.GetStore(sId).GetRegionSize() > storeFormSize {
			storeForm = sId
			storeFormSize = cluster.GetStore(sId).GetRegionSize()
		}
	}

	var storeToSize int64 = cluster.GetStore(storeTo).GetRegionSize()

	// 判断 storeForm Size 和 storeTo 的差是否大于 2 * GetAverageRegionSize
	var cSize int64 = storeFormSize - storeToSize
	if cSize < cluster.GetAverageRegionSize()*2 {
		return nil
	}

	// 创建 peer
	peer, err := cluster.AllocPeer(storeTo)
	if err != nil {
		panic(err)
	}

	// 移动
	peerOperator, err := operator.CreateMovePeerOperator(
		"",
		cluster,
		region,
		operator.OpBalance,
		storeForm,
		storeTo,
		peer.Id,
	)
	if err != nil {
		panic(err)
	}

	return peerOperator
}
