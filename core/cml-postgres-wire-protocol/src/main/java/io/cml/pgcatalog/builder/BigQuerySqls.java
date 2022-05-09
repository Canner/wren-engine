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

package io.cml.pgcatalog.builder;

import io.cml.spi.connector.Connector;

import java.util.List;

import static io.cml.pgcatalog.PgCatalogUtils.CML_TEMP_NAME;
import static java.lang.String.format;

public final class BigQuerySqls
{
    private BigQuerySqls() {}

    public static String createOrReplaceAllTable(Connector connector)
    {
        List<String> schemas = connector.listSchemas();
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.all_tables` AS ", CML_TEMP_NAME));
        for (String schema : schemas) {
            builder.append(format("SELECT * FROM `%s`.INFORMATION_SCHEMA.TABLES UNION ALL ", schema));
        }
        builder.setLength(builder.length() - "UNION ALL ".length());
        builder.append(";");
        return builder.toString();
    }

    public static String createOrReplaceAllColumn(Connector connector)
    {
        // TODO: we should check if pg_type has created or not.
        List<String> schemas = connector.listSchemas();
        StringBuilder builder = new StringBuilder();
        builder.append(format("CREATE OR REPLACE VIEW `%s.all_columns` AS ", CML_TEMP_NAME));
        for (String schema : schemas) {
            builder.append(format("SELECT col.column_name, col.ordinal_position, ptype.oid as typoid, ptype.typlen " +
                    "FROM `%s`.INFORMATION_SCHEMA.COLUMNS col, `pg_catalog.pg_type` ptype " +
                    "WHERE col.data_type = ptype.remotetype UNION ALL ", schema));
        }
        builder.setLength(builder.length() - "UNION ALL ".length());
        builder.append(";");
        return builder.toString();
    }
}
