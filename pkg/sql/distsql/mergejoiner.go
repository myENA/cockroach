// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type joinType int

const (
	innerJoin joinType = iota
	leftOuter
	rightOuter
	fullOuter
)

type mergeJoiner struct {
	inputs        []RowSource
	output        RowReceiver
	leftOrdering  sqlbase.ColumnOrdering
	rightOrdering sqlbase.ColumnOrdering
	outputCols    columns
	joinType      joinType
	onExpr        exprHelper
	ctx           context.Context
}

var _ processor = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx, spec *MergeJoinerSpec, inputs []RowSource, output RowReceiver,
) (*mergeJoiner, error) {
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
	}

	onExpr := exprHelper{}
	err := onExpr.init(spec.Expr, append(spec.LeftTypes, spec.RightTypes...), flowCtx.evalCtx)
	if err != nil {
		return nil, err
	}

	return &mergeJoiner{
		inputs:        inputs,
		output:        output,
		ctx:           log.WithLogTag(flowCtx.Context, "Merge Joiner", nil),
		outputCols:    columns(spec.OutputColumns),
		joinType:      joinType(spec.Type),
		onExpr:        onExpr,
		leftOrdering:  convertToColumnOrdering(spec.LeftOrdering),
		rightOrdering: convertToColumnOrdering(spec.RightOrdering),
	}, nil
}

func (m *mergeJoiner) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(m.ctx, "merge joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting merge joiner run")
		defer log.Infof(ctx, "exiting merge joiner run")
	}
}
