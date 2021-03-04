/**
 * Copyright 2017-2021 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.presto.rel2presto.functions;

import com.linkedin.coral.com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

public class JsonTupleToPrestoConverter {
    public static RexCall convert(RexBuilder builder, RexCall call) {
        RelDataType toDataType = call.getType();

        SqlOperator parseCast = new JsonParseCast(toDataType);
        return (RexCall) builder
                .makeCall(parseCast, call.getOperands().get(0));
    }

    private static class JsonParseCast extends GenericTemplateFunction {
        private final RelDataType resultType;

        JsonParseCast(RelDataType resultType)
        {
            super(resultType, "cast");
            this.resultType = resultType;
        }

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
        {
            Preconditions.checkState(call.operandCount() == 1, "Expected single operand");
            String castAsRow = RelDataTypeToPrestoTypeStringConverter.buildPrestoTypeString(resultType);
            writer.literal(String.format("CAST(json_parse(%s) as %s)", call.operand(0).toString(), castAsRow));
        }
    }
}
