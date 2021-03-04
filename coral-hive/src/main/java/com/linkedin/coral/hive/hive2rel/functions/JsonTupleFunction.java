/**
 * Copyright 2017-2021 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.hive.hive2rel.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;

import java.util.AbstractList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class JsonTupleFunction extends SqlUserDefinedFunction {
    public static final JsonTupleFunction JSON_TUPLE = new JsonTupleFunction();

    public JsonTupleFunction() {
        super(new SqlIdentifier("json_tuple", SqlParserPos.ZERO), null, null, null, null, null);
    }

    @Override
    public RelDataType inferReturnType(final SqlOperatorBinding opBinding) {
        checkState(opBinding instanceof SqlCallBinding);
        final SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        return opBinding.getTypeFactory().createStructType(new AbstractList<Map.Entry<String, RelDataType>>() {
            @Override
            public int size() {
                return opBinding.getOperandCount() - 1;
            }

            @Override
            public Map.Entry<String, RelDataType> get(int index) {
                String fieldName = callBinding.operand(index + 1).toString();
                String fieldNameNoQuotes = fieldName.substring(1, fieldName.length() - 1);
                return Pair.of(fieldNameNoQuotes, opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
            }
        });
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<SqlNode> operands = callBinding.operands();
        for (int i = 0; i < operands.size(); i++) {
            SqlNode fieldName = callBinding.operand(i);
            RelDataType colNameType = callBinding.getValidator().getValidatedNodeType(fieldName);
            if (SqlUtil.isNull(fieldName) || !SqlTypeFamily.STRING.contains(colNameType) || (i > 0 && !SqlUtil.isLiteral(fieldName))) {
                if (throwOnFailure) {
                    throw callBinding.newError(Static.RESOURCE.typeNotSupported(colNameType.toString()));
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    protected void checkOperandCount(SqlValidator validator, SqlOperandTypeChecker argTypeChecker, SqlCall call) {
        if (call.operandCount() > 1) {
            return;
        }
        throw validator.newValidationError(call, Static.RESOURCE.wrongNumOfArguments());
    }
}
