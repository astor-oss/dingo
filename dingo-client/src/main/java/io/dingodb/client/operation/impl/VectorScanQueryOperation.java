/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.client.operation.impl;

import io.dingodb.client.OperationContext;
import io.dingodb.client.common.IndexInfo;
import io.dingodb.client.common.VectorWithId;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class VectorScanQueryOperation implements Operation {

    private final static VectorScanQueryOperation INSTANCE = new VectorScanQueryOperation();

    public static VectorScanQueryOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, IndexInfo indexInfo) {
        VectorScanQuery query = parameters.getValue();
        NavigableSet<Task> subTasks = new TreeSet<>(Comparator.comparing(t -> t.getRegionId().entityId()));
        Map<DingoCommonId, Any> subTaskMap = new HashMap<>();

        List<RangeDistribution> rangeDistributions = new ArrayList<>(indexInfo.rangeDistribution.values());
        for (int i = 0; i < rangeDistributions.size(); i++) {
            RangeDistribution distribution = rangeDistributions.get(i);
            Map<DingoCommonId, VectorTuple<VectorScanQuery>> regionParam = subTaskMap.computeIfAbsent(
                distribution.getId(), k -> new Any(new HashMap<>())
            ).getValue();

            regionParam.put(distribution.getId(), new VectorTuple<>(i, query));
        }

        subTaskMap.forEach((k, v) -> subTasks.add(new Task(k, v)));
        return new Fork(new VectorWithIdArray[subTasks.size()], subTasks, false);
    }

    @Override
    public void exec(OperationContext context) {
        Map<DingoCommonId, VectorTuple<VectorScanQuery>> parameters = context.parameters();
        VectorScanQuery scanQuery = parameters.get(context.getRegionId()).v;
        List<io.dingodb.sdk.common.vector.VectorWithId> withIdList = context.getIndexService().vectorScanQuery(
            context.getIndexId(),
            context.getRegionId(),
            scanQuery
        );
        List<VectorWithId> result = withIdList.stream()
            .map(w -> new VectorWithId(w.getId(), w.getVector(), w.getScalarData()))
            .collect(Collectors.toList());

        context.<VectorWithIdArray[]>result()[parameters.get(context.getRegionId()).k] = new VectorWithIdArray(result, scanQuery.getIsReverseScan());
    }

    @AllArgsConstructor
    private static class VectorWithIdArray {
        public List<VectorWithId> vectorWithIds;
        public Boolean isReverseScan;

        public void addAll(List<VectorWithId> other) {
            vectorWithIds.addAll(other);
        }

        public List<VectorWithId> getVectorWithIds() {
            return vectorWithIds.stream()
                .filter(v -> v.getId() != 0)
                .sorted((v1, v2) ->
                    isReverseScan ? Long.compare(v2.getId(), v1.getId()) : Long.compare(v1.getId(), v2.getId()))
                .collect(Collectors.toList());
        }
    }

    @Override
    public <R> R reduce(Fork fork) {
        Boolean isReverseScan = Optional.mapOrGet(
            fork.getSubTasks(),
            __ -> __.first().<Map<DingoCommonId, VectorTuple<VectorScanQuery>>>parameters()
                .values()
                .stream()
                .findFirst()
                .get().v.getIsReverseScan(),
            () -> false);
        VectorWithIdArray withIdArray = new VectorWithIdArray(new ArrayList<>(), isReverseScan);
        Arrays.stream(fork.<VectorWithIdArray[]>result()).forEach(v -> withIdArray.addAll(v.vectorWithIds));
        return (R) withIdArray.getVectorWithIds();
    }
}
