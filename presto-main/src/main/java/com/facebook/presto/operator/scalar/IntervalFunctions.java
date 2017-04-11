/*
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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class IntervalFunctions
{
    private IntervalFunctions() {}

    @Description("convert duration to interval")
    @ScalarFunction("interval")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long intervalFromDuration(@SqlType("varchar(x)") Slice duration)
    {
        try {
            return Duration.valueOf(duration.toStringUtf8()).toMillis();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
