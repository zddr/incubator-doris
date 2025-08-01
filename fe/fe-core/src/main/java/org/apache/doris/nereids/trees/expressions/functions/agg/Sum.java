// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecisionForSum;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'sum'. This class is generated by GenerateFunction.
 */
public class Sum extends NullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, ComputePrecisionForSum, SupportWindowAnalytic,
        RollUpTrait, SupportMultiDistinct {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(FloatType.INSTANCE),
            FunctionSignature.ret(DecimalV3Type.WILDCARD).args(DecimalV3Type.WILDCARD),
            FunctionSignature.ret(LargeIntType.INSTANCE).args(LargeIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BooleanType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public Sum(Expression arg) {
        this(false, false, false, arg);
    }

    /**
     * constructor with 1 argument.
     */
    public Sum(boolean distinct, Expression arg) {
        this(distinct, false, false, arg);
    }

    public Sum(boolean distinct, boolean alwaysNullable, Expression arg) {
        super("sum", distinct, alwaysNullable, arg);
    }

    public Sum(boolean distinct, boolean alwaysNullable, boolean isSkew, Expression arg) {
        super("sum", distinct, alwaysNullable, isSkew, arg);
    }

    @Override
    public MultiDistinctSum convertToMultiDistinct() {
        Preconditions.checkArgument(distinct,
                "can't convert to multi_distinct_sum because there is no distinct args");
        return new MultiDistinctSum(false, alwaysNullable, child());
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType argType = child().getDataType();
        if (!argType.isNumericType() && !argType.isBooleanType()
                && !argType.isNullType() && !argType.isStringLikeType()) {
            throw new AnalysisException("sum requires a numeric, boolean or string parameter: " + this.toSql());
        }
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public Sum withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Sum(distinct, alwaysNullable, isSkew, children.get(0));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new Sum(distinct, alwaysNullable, isSkew, children.get(0));
    }

    @Override
    public Expression withIsSkew(boolean isSkew) {
        return new Sum(distinct, alwaysNullable, isSkew, child());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSum(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        if (getArgument(0).getDataType() instanceof NullType) {
            return FunctionSignature.ret(BigIntType.INSTANCE).args(TinyIntType.INSTANCE);
        } else if (getArgument(0).getDataType() instanceof DecimalV2Type) {
            return FunctionSignature.ret(DecimalV3Type.WILDCARD).args(DecimalV3Type.WILDCARD);
        }
        return ExplicitlyCastableSignature.super.searchSignature(signatures);
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        return new Sum(this.distinct, param);
    }

    @Override
    public boolean canRollUp() {
        return true;
    }
}
