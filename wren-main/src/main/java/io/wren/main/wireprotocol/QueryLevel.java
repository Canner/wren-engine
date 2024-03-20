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

package io.wren.main.wireprotocol;

public enum QueryLevel
{
    // The metadata query is related to PG metadata and fully supported by Metastore,
    // it will pass to Metastore without any modification and executed by Metastore.
    METASTORE_FULL,
    // The metadata query is related to PG metadata and partially supported by Metastore,
    // it will be rewritten by Wren and executed by Metastore.
    METASTORE_SEMI,
    // The query is related to real data, it will be rewritten by Wren and executed by Data Source.
    DATASOURCE
}
