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

grammar ParameterListBase;

parameterList
    : '(' (parameter (',' parameter)*)? ')'
    ;

parameter
    : paraName ':' typeName
    ;

typeName
    : ID
    ;

paraName
    : ID
    ;

ID : [a-zA-Z_][a-zA-Z0-9_]*;


fragment WhiteSpace
    : '\u0020'
    | '\u0009'
    | '\u000D'
    | '\u000A'
    ;


WS
    : WhiteSpace+ -> skip
    ;