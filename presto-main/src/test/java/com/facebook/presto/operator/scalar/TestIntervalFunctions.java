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

import com.facebook.presto.type.SqlIntervalDayTime;
import org.testng.annotations.Test;

import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;

public class TestIntervalFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIntervalFromDuration()
    {
        assertFunction("interval('1234 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("interval('1234 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("interval('1234 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("interval('1234 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("interval('1234 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("interval('1234 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("interval('1234.567 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("interval('1234.567 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("interval('1234.567 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("interval('1234.567 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("interval('1234.567 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("interval('1234.567 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // no spaces
        assertFunction("interval('1234ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("interval('1234ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("interval('1234s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("interval('1234m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("interval('1234h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("interval('1234d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("interval('1234.567ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("interval('1234.567ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("interval('1234.567s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("interval('1234.567m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("interval('1234.567h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("interval('1234.567d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // invalid function calls
        assertInvalidFunction("interval('')", "duration is empty");
        assertInvalidFunction("interval('1f')", "Unknown time unit: f");
        assertInvalidFunction("interval('abc')", "duration is not a valid data duration string: abc");
    }
}
