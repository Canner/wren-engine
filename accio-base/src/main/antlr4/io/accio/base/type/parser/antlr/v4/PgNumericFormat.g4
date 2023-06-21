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

grammar PgNumericFormat;

format
    : pattern (pattern)*
    ;

pattern
    : DIGIT_CAN_DROP            #digitPattern
    | DIGIT_CAN_NOT_DROP        #digitPattern
    | DECIMAL_POINT             #decimalPointPattern
    | DECIMAL_POINT_LOCALE      #decimalPointPattern
    | GROUP_SEPARATOR           #groupSeparatorPattern
    | GROUP_SEPARATOR_LOCALE    #groupSeparatorPattern
    | CURRENCY_SYMBOL           #currencySymbolPattern
    | EXPONENT                  #exponentPattern
    | nonReserved               #nonReservedPattern
    ;

nonReserved
    : PR
    | SHIFT_DIGIT
    | ORDINAL
    | ROMAN
    | PLUS
    | MINUS
    | SIGN
    | SIGN_ANCHOR_LOCALE
    | FM
    ;

DIGIT_CAN_DROP: '9';
DIGIT_CAN_NOT_DROP: '0';
DECIMAL_POINT: '.';
GROUP_SEPARATOR: ',';
PR: 'PR';
SIGN_ANCHOR_LOCALE: 'S';
CURRENCY_SYMBOL: 'L';
DECIMAL_POINT_LOCALE: 'D';
GROUP_SEPARATOR_LOCALE: 'G';
MINUS: 'MI';
PLUS: 'PL';
SIGN: 'SG';
ROMAN: 'RN';
ORDINAL: 'TH' | 'th';
SHIFT_DIGIT: 'V';
EXPONENT: 'EEEE';
FM: 'FM';
