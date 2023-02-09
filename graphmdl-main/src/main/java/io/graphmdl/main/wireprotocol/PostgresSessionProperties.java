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

package io.graphmdl.main.wireprotocol;

import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;
import static org.apache.commons.lang3.StringUtils.isNumeric;
import static org.elasticsearch.common.Booleans.isBoolean;

public final class PostgresSessionProperties
{
    private PostgresSessionProperties() {}

    public static final String SERVER_VERSION = "server_version";
    public static final String SERVER_ENCODING = "server_encoding";
    public static final String IS_SUPER_USER = "is_superuser";
    public static final String SESSION_AUTHORIZATION = "session_authorization";
    public static final String INTERVALSTYLE = "intervalstyle";
    public static final String TIMEZONE = "TimeZone";
    public static final String INTEGER_DATETIMES = "integer_datetimes";
    public static final String STANDARD_CONFORMING_STRINGS = "standard_conforming_strings";
    public static final String APPLICATION_NAME = "application_name";

    public static final String EXTRA_FLOAT_DIGITS = "extra_float_digits";
    public static final String DATE_STYLE = "datestyle";
    public static final String CLIENT_ENCODING = "client_encoding";

    /**
     * unsupported server configuration list
     * see https://www.postgresql.org/docs/13/runtime-config.html
     */
    private static final Set<String> ignoredSessionProperties = Set.of(
            /* File Locations */
            "data_directory",
            "config_file",
            "hba_file",
            "ident_file",
            "external_pid_file",
            /* Connections and Authentication / Connection Settings */
            "listen_addresses",
            "port",
            "max_connections",
            "superuser_reserved_connections",
            "unix_socket_directories",
            "unix_socket_group",
            "unix_socket_permissions",
            "bonjour",
            "bonjour_name",
            "tcp_keepalives_idle",
            "tcp_keepalives_interval",
            "tcp_keepalives_count",
            "tcp_user_timeout",
            /* Connections and Authentication / Authentication */
            "authentication_timeout",
            "password_encryption",
            "krb_server_keyfile",
            "krb_caseins_users",
            "db_user_namespace",
            /* Connections and Authentication / SSL */
            "ssl",
            "ssl_ca_file",
            "ssl_cert_file",
            "ssl_crl_file",
            "ssl_key_file",
            "ssl_ciphers",
            "ssl_prefer_server_ciphers",
            "ssl_ecdh_curve",
            "ssl_min_protocol_version",
            "ssl_max_protocol_version",
            "ssl_dh_params_file",
            "ssl_passphrase_command",
            "ssl_passphrase_command_supports_reload",
            /* Resource Consumption / Memory */
            "shared_buffers",
            "huge_pages",
            "temp_buffers",
            "max_prepared_transactions",
            "work_mem",
            "hash_mem_multiplier",
            "maintenance_work_mem",
            "autovacuum_work_mem",
            "logical_decoding_work_mem",
            "max_stack_depth",
            "shared_memory_type",
            "dynamic_shared_memory_type",
            /* Resource Consumption / Disk */
            "temp_file_limit",
            /* Resource Consumption / Kernel Resource Usage */
            "max_files_per_process",
            /* Resource Consumption / Cost-based Vacuum Delay */
            "vacuum_cost_delay",
            "vacuum_cost_page_hit",
            "vacuum_cost_page_miss",
            "vacuum_cost_page_dirty",
            "vacuum_cost_limit",
            /* Resource Consumption / Background Writer */
            "bgwriter_delay",
            "bgwriter_lru_maxpages",
            "bgwriter_lru_multiplier",
            "bgwriter_flush_after",
            /* Resource Consumption / Asynchronous Behavior */
            "effective_io_concurrency",
            "maintenance_io_concurrency",
            "max_worker_processes",
            "max_parallel_workers_per_gather",
            "max_parallel_maintenance_workers",
            "max_parallel_workers",
            "backend_flush_after",
            "old_snapshot_threshold",
            /* Write Ahead Log / Settings */
            "wal_level",
            "fsync",
            "synchronous_commit",
            "wal_sync_method",
            "full_page_writes",
            "wal_log_hints",
            "wal_compression",
            "wal_init_zero",
            "wal_recycle",
            "wal_buffers",
            "wal_writer_delay",
            "wal_writer_flush_after",
            "wal_skip_threshold",
            "commit_delay",
            "commit_siblings",
            /* Write Ahead Log / Checkpoints */
            "checkpoint_timeout",
            "checkpoint_completion_target",
            "checkpoint_flush_after",
            "checkpoint_warning",
            "max_wal_size",
            "min_wal_size",
            /* Write Ahead Log / Archiving */
            "archive_mode",
            "archive_command",
            "archive_timeout",
            /* Write Ahead Log / Archive Recovery */
            "restore_command",
            "archive_cleanup_command",
            "recovery_end_command",
            /* Write Ahead Log / Recovery Target */
            "recovery_target",
            "recovery_target_name",
            "recovery_target_time",
            "recovery_target_xid",
            "recovery_target_lsn",
            "recovery_target_inclusive",
            "recovery_target_timeline",
            "recovery_target_action",
            /* Replication / Sending Servers */
            "max_wal_senders",
            "max_replication_slots",
            "wal_keep_size",
            "max_slot_wal_keep_size",
            "wal_sender_timeout",
            "track_commit_timestamp",
            /* Replication / Master Server */
            "synchronous_standby_names",
            "vacuum_defer_cleanup_age",
            /* Replication / Standby Servers */
            "primary_conninfo",
            "primary_slot_name",
            "promote_trigger_file",
            "hot_standby",
            "max_standby_archive_delay",
            "max_standby_streaming_delay",
            "wal_receiver_create_temp_slot",
            "wal_receiver_status_interval",
            "hot_standby_feedback",
            "wal_receiver_timeout",
            "wal_retrieve_retry_interval",
            "recovery_min_apply_delay",
            /* Replication / Subscribers */
            "max_logical_replication_workers",
            "max_sync_workers_per_subscription",
            /* Query Planning / Planner Method Configuration */
            "enable_bitmapscan",
            "enable_gathermerge",
            "enable_hashagg",
            "enable_hashjoin",
            "enable_incremental_sort",
            "enable_indexscan",
            "enable_indexonlyscan",
            "enable_material",
            "enable_mergejoin",
            "enable_nestloop",
            "enable_parallel_append",
            "enable_parallel_hash",
            "enable_partition_pruning",
            "enable_partitionwise_join",
            "enable_partitionwise_aggregate",
            "enable_seqscan",
            "enable_sort",
            "enable_tidscan",
            /* Query Planning / Planner Cost Constants */
            "seq_page_cost",
            "random_page_cost",
            "cpu_tuple_cost",
            "cpu_index_tuple_cost",
            "cpu_operator_cost",
            "parallel_setup_cost",
            "parallel_tuple_cost",
            "min_parallel_table_scan_size",
            "min_parallel_index_scan_size",
            "effective_cache_size",
            "jit_above_cost",
            "jit_inline_above_cost",
            "jit_optimize_above_cost",
            /* Query Planning / Genetic Query Optimizer */
            "geqo",
            "geqo_threshold",
            "geqo_effort",
            "geqo_pool_size",
            "geqo_generations",
            "geqo_selection_bias",
            "geqo_seed",
            /* Query Planning / Other Planner Options */
            "default_statistics_target",
            "constraint_exclusion",
            "cursor_tuple_fraction",
            "from_collapse_limit",
            "jit",
            "join_collapse_limit",
            "parallel_leader_participation",
            "force_parallel_mode",
            "plan_cache_mode",
            /* Error Reporting and Logging / Where to Log */
            "log_destination",
            "logging_collector",
            "log_directory",
            "log_filename",
            "log_file_mode",
            "log_rotation_age",
            "log_rotation_size",
            "log_truncate_on_rotation",
            "syslog_facility",
            "syslog_ident",
            "syslog_sequence_numbers",
            "syslog_split_messages",
            "event_source",
            /* Error Reporting and Logging / When to Log */
            "log_min_messages",
            "log_min_error_statement",
            "log_min_duration_statement",
            "log_min_duration_sample",
            "log_statement_sample_rate",
            "log_transaction_sample_rate",
            /* Error Reporting and Logging / What to Log */
            APPLICATION_NAME,
            "debug_print_parse",
            "debug_print_rewritten",
            "debug_print_plan",
            "debug_pretty_print",
            "log_checkpoints",
            "log_connections",
            "log_disconnections",
            "log_duration",
            "log_error_verbosity",
            "log_hostname",
            "log_line_prefix",
            "log_lock_waits",
            "log_parameter_max_length",
            "log_parameter_max_length_on_error",
            "log_statement",
            "log_replication_commands",
            "log_temp_files",
            "log_timezone",
            /* Error Reporting and Logging / Process Title */
            "cluster_name",
            "update_process_title",
            /* Run-time Statistics / Query and Index Statistics Collector */
            "track_activities",
            "track_activity_query_size",
            "track_counts",
            "track_io_timing",
            "track_functions",
            "stats_temp_directory",
            /* Run-time Statistics / Statistics Monitoring */
            "log_statement_stats",
            "log_parser_stats",
            "log_planner_stats",
            "log_executor_stats",
            /* Automatic Vacuuming */
            "autovacuum",
            "log_autovacuum_min_duration",
            "autovacuum_max_workers",
            "autovacuum_naptime",
            "autovacuum_vacuum_threshold",
            "autovacuum_vacuum_insert_threshold",
            "autovacuum_analyze_threshold",
            "autovacuum_vacuum_scale_factor",
            "autovacuum_vacuum_insert_scale_factor",
            "autovacuum_analyze_scale_factor",
            "autovacuum_freeze_max_age",
            "autovacuum_multixact_freeze_max_age",
            "autovacuum_vacuum_cost_delay",
            "autovacuum_vacuum_cost_limit",
            /* Client Connection Defaults / Statement Behavior */
            "client_min_messages",
            "search_path",
            "row_security",
            "default_table_access_method",
            "default_tablespace",
            "temp_tablespaces",
            "check_function_bodies",
            "default_transaction_isolation",
            "default_transaction_read_only",
            "default_transaction_deferrable",
            "session_replication_role",
            "statement_timeout",
            "lock_timeout",
            "idle_in_transaction_session_timeout",
            "vacuum_freeze_table_age",
            "vacuum_freeze_min_age",
            "vacuum_multixact_freeze_table_age",
            "vacuum_multixact_freeze_min_age",
            "bytea_output",
            "xmlbinary",
            "xmloption",
            "gin_pending_list_limit",
            /* Client Connection Defaults / Locale and Formatting */
            INTERVALSTYLE,
            TIMEZONE,
            "timezone_abbreviations",
            "lc_messages",
            "lc_monetary",
            "lc_numeric",
            "lc_time",
            "default_text_search_config",
            /* Client Connection Defaults / Shared Library Preloading */
            "local_preload_libraries",
            "session_preload_libraries",
            "shared_preload_libraries",
            "jit_provider",
            /* Client Connection Defaults / Other Defaults */
            "dynamic_library_path",
            "gin_fuzzy_search_limit",
            /* Lock Management */
            "deadlock_timeout",
            "max_locks_per_transaction",
            "max_pred_locks_per_transaction",
            "max_pred_locks_per_relation",
            "max_pred_locks_per_page",
            /* Version and Platform Compatibility */
            "array_nulls",
            "backslash_quote",
            "escape_string_warning",
            "lo_compat_privileges",
            "operator_precedence_warning",
            "quote_all_identifiers",
            STANDARD_CONFORMING_STRINGS,
            "synchronize_seqscans",
            "transform_null_equals",
            /* Error Handling */
            "exit_on_error",
            "restart_after_crash",
            "data_sync_retry",
            /* Preset Options */
            "block_size",
            "data_checksums",
            "data_directory_mode",
            "debug_assertions",
            INTEGER_DATETIMES,
            "lc_collate",
            "lc_ctype",
            "max_function_args",
            "max_identifier_length",
            "max_index_keys",
            "segment_size",
            SERVER_ENCODING,
            SERVER_VERSION,
            "server_version_num",
            "ssl_library",
            "wal_block_size",
            "wal_segment_size",
            /* Developer Options */
            "allow_system_table_mods",
            "backtrace_functions",
            "ignore_system_indexes",
            "post_auth_delay",
            "pre_auth_delay",
            "trace_notify",
            "trace_recovery_messages",
            "trace_sort",
            "trace_locks",
            "trace_lwlocks",
            "trace_userlocks",
            "trace_lock_oidmin",
            "trace_lock_table",
            "debug_deadlocks",
            "log_btree_build_stats",
            "wal_consistency_checking",
            "wal_debug",
            "ignore_checksum_failure",
            "zero_damaged_pages",
            "ignore_invalid_pages",
            "jit_debugging_support",
            "jit_dump_bitcode",
            "jit_expressions",
            "jit_profiling_support",
            "jit_tuple_deforming",
            /* other */
            IS_SUPER_USER,
            SESSION_AUTHORIZATION,
            EXTRA_FLOAT_DIGITS);

    /**
     * hard-wired parameters
     * See https://www.postgresql.org/docs/13/protocol-flow.html
     */
    private static final Set<String> hardWiredSessionProperties = Set.of(
            SERVER_VERSION,
            SERVER_ENCODING,
            CLIENT_ENCODING,
            IS_SUPER_USER,
            SESSION_AUTHORIZATION,
            DATE_STYLE,
            INTERVALSTYLE,
            TIMEZONE,
            INTEGER_DATETIMES,
            STANDARD_CONFORMING_STRINGS,
            APPLICATION_NAME);

    public static final Function<String, String> QUOTE_STRATEGY = text -> isQuoted(text) ? text : "'" + text + "'";
    public static final Function<String, String> UNQUOTE_STRATEGY = text -> isQuoted(text) ? text.substring(1, text.length() - 1) : text;

    public static boolean isIgnoredSessionProperties(String property)
    {
        return ignoredSessionProperties.contains(property.toLowerCase(ENGLISH));
    }

    public static boolean isHardWiredSessionProperty(String property)
    {
        return hardWiredSessionProperties.contains(property.toLowerCase(ENGLISH));
    }

    public static String formatValue(String value, Function<String, String> strategy)
    {
        if (isNumeric(value) ||
                isBoolean(value)) {
            return value;
        }

        return splitString(value).stream()
                .map(strategy)
                .collect(Collectors.joining(", "));
    }

    private static List<String> splitString(String value)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        Queue<Character> queue = new LinkedList<>();
        int counter = 0;

        for (char c : value.toCharArray()) {
            if (isCompleteParameter(queue, counter, c)) {
                builder.add(combineToString(queue));
                queue.clear();
                counter = 0;
                continue;
            }

            if (c == '\'') {
                counter++;
            }
            queue.add(c);
        }

        // add last parameter
        if (!queue.isEmpty()) {
            builder.add(combineToString(queue));
        }

        return builder.build();
    }

    private static boolean isCompleteParameter(Queue<Character> queue, int counter, char c)
    {
        return c == ',' && !queue.isEmpty() && (queue.peek() != '\'' || counter % 2 == 0);
    }

    private static String combineToString(Queue<Character> list)
    {
        return list.stream().map(Object::toString).collect(Collectors.joining("")).trim();
    }

    private static boolean isQuoted(String text)
    {
        return text.charAt(0) == '\'' && text.charAt(text.length() - 1) == '\'';
    }
}
