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

package io.dingodb.exec.fun.time;

import io.dingodb.exec.utils.DingoDateTimeUtils;
import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;

@Evaluators(
    induceSequence = {
        long.class,
        double.class,
        BigDecimal.class,
        int.class,
    }
)
final class UnixTimestampEvaluators {
    private UnixTimestampEvaluators() {
    }

    static long unixTimestamp(@NonNull Timestamp value) {
        return DateTimeUtils.toSecond(value.getTime())
            .setScale(0, RoundingMode.HALF_UP)
            .longValue();
    }

    static long unixTimestamp() {
        Timestamp value = DingoDateTimeUtils.currentTimestamp();
        return unixTimestamp(value);
    }

    static long unixTimestamp(long value) {
        return value;
    }
}
