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

package io.dingodb.calcite.grammar;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.apache.calcite.sql2rel.SqlVectorOperator;

public class SqlUserDefinedOperators {
    public static SqlLikeBinaryOperator LIKE_BINARY =
        new SqlLikeBinaryOperator("LIKE_BINARY", SqlKind.OTHER_FUNCTION, false, true);

    public static SqlLikeBinaryOperator NOT_LIKE_BINARY =
        new SqlLikeBinaryOperator("NOT LIKE_BINARY", SqlKind.OTHER_FUNCTION, true, true);

    public static SqlFunctionScanOperator SCAN = new SqlFunctionScanOperator("SCAN", SqlKind.COLLECTION_TABLE);

    public static SqlVectorOperator VECTOR = new SqlVectorOperator("VECTOR", SqlKind.COLLECTION_TABLE);
}
