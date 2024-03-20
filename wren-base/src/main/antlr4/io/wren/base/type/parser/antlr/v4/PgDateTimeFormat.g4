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

grammar PgDateTimeFormat;

format
    : symbol (symbol)*
    ;

symbol
    : separator
    | pattern
    ;

pattern
    : HOUR           #hourLiteral
    | MINUTE         #minuteLiteral
    | SECOND         #secondLiteral
    | MILLISECOND    #milliSecondLiteral
    | YEAR           #yearLiteral
    | MONTH          #monthLiteral
    | DAY            #dayLiteral
    | WEEK           #weekLiteral
    | TIME_ZONE      #timeZoneLiteral
    | AM             #meridiemMarkerLiteral
    | PM             #meridiemMarkerLiteral
    | BC             #eraDesignatorLiteral
    | AD             #eraDesignatorLiteral
    ;

separator
    : SEPARATOR
    ;

HOUR: ('HH' | 'hh') ('12' | '24')?;
MINUTE: 'MI' | 'mi';
SECOND: 'SS' | 'ss';
MILLISECOND: 'MS' | 'ms' | 'FF3' | 'ff3';
YEAR: 'YYYY' | 'yyyy' | 'YYY' | 'yyy' | 'YY' | 'yy' | 'Y' | 'y';
MONTH: 'MM' | 'mm' | 'Month' | 'Mon';
DAY: 'DDD' | 'ddd' | 'DD' | 'dd' | 'D' | 'd' | 'Day' | 'Dy';
WEEK: 'WW' | 'ww';
TIME_ZONE: 'TZ';
AM: 'AM' | 'am';
PM: 'PM' | 'pm';
BC: 'BC' | 'bc';
AD: 'AD' | 'ad';

SEPARATOR: [ _/:\-\\.];
