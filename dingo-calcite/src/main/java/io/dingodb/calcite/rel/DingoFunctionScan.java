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

package io.dingodb.calcite.rel;

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DingoFunctionScan extends LogicalTableFunctionScan implements DingoRel {
    public DingoFunctionScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        RexNode rexCall,
        @Nullable Type elementType, RelDataType rowType,
        @Nullable Set<RelColumnMapping> columnMappings
    ) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@NonNull RelOptPlanner planner, @NonNull RelMetadataQuery mq) {
        // Assume that part scan has half cost.
        return Objects.requireNonNull(super.computeSelfCost(planner, mq));
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        // crucial, this is how Calcite distinguish between different node with different props.
        return pw;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
