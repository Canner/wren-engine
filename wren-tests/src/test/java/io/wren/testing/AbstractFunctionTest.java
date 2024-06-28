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

package io.wren.testing;

import io.wren.base.dto.Manifest;
import io.wren.main.web.dto.PreviewDto;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Test for the list of <a href="https://trino.io/docs/current/functions/list-by-topic.html">Trino functions</a>
 */

public abstract class AbstractFunctionTest
        extends RequireWrenServer
{
    @DataProvider
    public Object[][] dateTimeFunction()
    {
        return new Object[][] {
                {"SELECT current_date"},
                // {"SELECT current_time",},
                {"SELECT current_timestamp"},
                // {"SELECT localtime",},
                // {"SELECT localtimestamp"},
                // {"SELECT current_timezone"},
                // {"SELECT date('2020-05-01')"},
                // {"SELECT date_add('second', 86, TIMESTAMP '2020-03-01 00:00:00')"},
                // {"SELECT date_diff('second', TIMESTAMP '2020-03-01 00:00:00', TIMESTAMP '2020-03-02 00:00:00')"},
                // {"SELECT date_format(TIMESTAMP '2022-10-20 05:10:00', '%m-%d-%Y %H')"},
                // {"SELECT date_parse('2022/10/20/05', '%Y/%m/%d/%H')"},
                {"SELECT date_trunc('MONTH' , DATE '2022-10-20')"},
                {"SELECT date_trunc('DAY' , TIMESTAMP '2022-10-20 00:00:00')"},
                // {"SELECT from_iso8601_date('2020-05-11')"},
                // {"SELECT from_iso8601_timestamp('2020-05-11')"},
                // {"SELECT from_unixtime_nanos(100)"},
                // {"SELECT from_unixtime(123456)"},
                // {"SELECT from_unixtime(123456, 'UTC')"},
                // {"SELECT now()"},
                // {"SELECT to_iso8601(TIMESTAMP '2022-11-01 09:08:07.321')"},
                // {"SELECT to_milliseconds(INTERVAL '1' month)"},
                // {"SELECT to_unixtime(TIMESTAMP '2022-11-01 09:08:07.321')"},
        };
    }

    @Test(dataProvider = "dateTimeFunction")
    public void testDateTimeFunction(String sql)
    {
        assertThatNoException().isThrownBy(() -> execute(sql));
    }

    @DataProvider
    public Object[][] aggregationFunction()
    {
        return new Object[][] {
                // {"select any_value(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select approx_distinct(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select approx_most_frequent(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select approx_percentile(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select arbitrary(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                {"select array_agg(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                {"select avg(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select bitwise_and_agg(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select bitwise_or_agg(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select bool_and(c1) from (values (true, false), (true, true)) t(c1, c2)"},
                {"select bool_or(c1) from (values (true, false), (true, true)) t(c1, c2)"},
                // {"select checksum(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select corr(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                {"select count(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select count_if(c1 > 1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select count_if(c1 > 1) filter (where c1 > 1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select covar_pop(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select covar_samp(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select every(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select geometric_mean(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select histogram(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select kurtosis(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // map_agg
                // map_union
                {"select max(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select max_by(c1, c2) from (values (1,2), (2,3)) t(c1, c2)"},
                {"select min(c1) from (values (1,2), (2,3)) t(c1, c2)"},
                // {"select min_by(c1, c2) from (values (1,2), (2,3)) t(c1, c2)"},
                {"select sum(c1) from (values (1,2), (2,3)) t(c1, c2)"},
        };
    }

    @Test(dataProvider = "aggregationFunction")
    public void testAggregationFunction(String sql)
    {
        assertThatNoException().isThrownBy(() -> execute(sql));
    }

    @DataProvider
    public Object[][] mathFunction()
    {
        return new Object[][] {
                {"select greatest(1, 2)"},
                {"select least(1, 2)"},
                {"select abs(-1)"},
                {"select acos(0.5)"},
                {"select asin(0.5)"},
                {"select atan(0.5)"},
                {"select atan2(0.5, 0.5)"},
                // {"select beta_cdf(1, 5, 1)"},
                {"select cbrt(8)"},
                {"select ceil(1.5)"},
                {"select cos(0.5)"},
                // {"select cosh(0.5)"},
                // {"SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0]))"},
                // {"select degrees(0.5)"},
                // {"select e()"},
                {"select exp(0.5)"},
                {"select floor(1.5)"},
                // {"select from_base('123', 10)"},
                // {"select infinity()"},
                // inverse_veta_cdf
                // inverse_normal_cdf
                // {"select is_finite(1)"},
                // {"select is_nan(1)"},
                {"select ln(0.5)"},
                {"select log(2, 8)"},
                // {"select log10(100)"},
                {"select mod(5, 2)"},
                // {"select nan()"},
                // normal_cdf
                // {"select pi()"},
                {"select power(2, 3)"},
                // {"select radians(0.5)"},
                {"select round(1.5)"},
                {"select sign(-1)"},
                {"select sin(0.5)"},
                // {"select sinh(0.5)"},
                {"select sqrt(4)"},
                {"select tan(0.5)"},
                // {"select tanh(0.5)"},
                // {"select to_base(123, 10)"},
                // {"select truncate(1.5)"},
                // {"select width_bucket(1.5, 1, 2, 3)"},
                // {"select wilson_interval_lower(1, 2, 1)"},
                // {"select wilson_interval_upper(1, 2, 1)"},
        };
    }

    @Test(dataProvider = "mathFunction")
    public void testMathFunction(String sql)
    {
        assertThatNoException().isThrownBy(() -> execute(sql));
    }

    @DataProvider
    public Object[][] stringFunction()
    {
        return new Object[][] {
                {"select chr(1)"},
                // {"select codepoint('123')"},
                {"select concat('123', 'abc')"},
                // {"select concat_ws(',', '123', 'abc')"},
                {"select format('%s %s', '123', 'abc')"},
                // {"select from_utf8('MTIz')"},
                // {"select hamming_distance('123', 'abc')"},
                {"select length('MTIz')"},
                // {"select levenshtein_distance('123,'abc')"},
                {"select lower('MTIz')"},
                {"select length('MTIz')"},
                {"select lpad('MTIz', 5, ' ')"},
                {"select ltrim('MTIz')"},
                // {"select luhn_check('79927398713')"},
                // {"select normalize('79927398713')"},
                {"select position('123' in '123456')"},
                // {"select replace('1', '123')"},
                {"select replace('1', '123', '2')"},
                {"select reverse('MTIz')"},
                {"select rpad('MTIz', 5, ' ')"},
                {"select rtrim('MTIz')"},
                // {"select soundex('abc')"},
                // {"select split('abc', 'b')"},
                // {"select split_part('abc', 'b', 1)"},
                // split_to_multimap
                {"select strpos('abc', 'b')"},
                {"select substr('abc', 1, 2)"},
                {"select substring('abc', 1, 2)"},
                // {"select to_utf8('MTIz')"},
                {"select translate('abc', 'b', 'd')"},
                {"select trim('abc')"},
                {"select upper('abc')"},
                // {"select split_to_multimap('abc')"},
        };
    }

    @Test(dataProvider = "stringFunction")
    public void testStringFunction(String sql)
    {
        assertThatNoException().isThrownBy(() -> execute(sql));
    }

    @DataProvider
    public Object[][] operators()
    {
        return new Object[][] {
                {"select 1 + 1"},
                {"select 1 - 1"},
                {"select 2 * 2"},
                {"select 4 / 2"},
                // {"select 11 % 10"},
                // {"select a1[1] from (values (array[1,2,3])) t(a1)"},
                // {"select a1 || a1 from (values (array[1,2,3])) t(a1)"},
                {"select '123' || '456'"},
                {"select 2 < 2"},
                {"select 2 > 2"},
                {"select 2 >= 2"},
                {"select 2 <= 2"},
                {"select 2 = 2"},
                {"select 2 <> 2"},
                {"select 2 != 2"},
        };
    }

    @Test(dataProvider = "operators")
    public void testOperators(String sql)
    {
        assertThatNoException().isThrownBy(() -> execute(sql));
    }

    protected void execute(String sql)
    {
        preview(new PreviewDto(Manifest.builder().setCatalog("wrenai").setSchema("public").build(), sql, 100L));
    }
}
