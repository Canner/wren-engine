use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions::string::lower;
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use wren_core::array::AsArray;
use wren_core::array::GenericByteArray;
use wren_core::array::GenericListArray;
use wren_core::datatypes::DataType;
use wren_core::datatypes::GenericStringType;
use wren_core::mdl::function::ByPassScalarUDF;
use wren_core::mdl::function::FunctionType;
use wren_core::mdl::function::RemoteFunction;
use wren_core::ScalarUDF;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let sql = r#"
            WITH inputs AS (
                SELECT
                    r.specific_name,
                    r.data_type as return_type,
                    pi.rid,
                    array_agg(pi.parameter_name order by pi.ordinal_position) as param_names,
                    array_agg(pi.data_type order by pi.ordinal_position) as param_types
                FROM
                    information_schema.routines r
                JOIN
                    information_schema.parameters pi ON r.specific_name = pi.specific_name AND pi.parameter_mode = 'IN'
                GROUP BY 1, 2, 3
            )
            SELECT
                r.routine_name as name,
                i.param_names,
                i.param_types,
                r.data_type as return_type,
                r.function_type,
                r.description
            FROM
                information_schema.routines r
            LEFT JOIN
                inputs i ON r.specific_name = i.specific_name
        "#;
    let config = SessionConfig::new().with_information_schema(true);
    let state: datafusion::execution::SessionState = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();
    let ctx = SessionContext::new_with_state(state);
    // ctx.register_udaf(Arc::unwrap_or_clone(array_agg_udaf()));
    // ctx.register_udf(ScalarUDF::new_from_impl(lower::LowerFunc::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
        "add_two",
        DataType::Int64,
    )));
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut functions = vec![];

    for batch in batches {
        let name_array = batch.column(0).as_string::<i32>();
        let param_names_array = batch.column(1).as_list::<i32>();
        let param_types_array = batch.column(2).as_list::<i32>();
        let return_type_array = batch.column(3).as_string::<i32>();
        let function_type_array = batch.column(4).as_string::<i32>();
        let description_array = batch.column(5).as_string::<i32>();

        for row in 0..batch.num_rows() {
            let name = name_array.value(row).to_string();
            let _param_names =
                to_string_vec(param_names_array.value(row).as_string::<i32>());
            let _param_types =
                to_string_vec(param_types_array.value(row).as_string::<i32>());
            let return_type = return_type_array.value(row).to_string();
            let description = description_array.value(row).to_string();
            let function_type = function_type_array.value(row).to_string();

            functions.push(RemoteFunction {
                name,
                param_names: None,
                param_types: None,
                return_type,
                description: Some(description),
                function_type: FunctionType::from_str(&function_type).unwrap(),
            });
        }
    }
    functions
        .iter()
        .filter(|f| f.name == "add_two")
        .for_each(|f| {
            println!("{:?}", f);
        });
    Ok(())
}

fn to_string_vec(
    array: &GenericByteArray<GenericStringType<i32>>,
) -> Vec<Option<String>> {
    array
        .iter()
        .map(|s| s.map(|s| s.to_string()))
        .collect::<Vec<Option<String>>>()
}

fn read_remote_function_list(path: &str) -> Vec<RemoteFunction> {
    csv::Reader::from_path(path)
        .unwrap()
        .into_deserialize::<PyRemoteFunction>()
        .filter_map(Result::ok)
        .map(|f| RemoteFunction::from(f))
        .collect::<Vec<RemoteFunction>>()
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PyRemoteFunction {
    pub function_type: String,
    pub name: String,
    pub return_type: Option<String>,
    /// It's a comma separated string of parameter names
    pub param_names: Option<String>,
    /// It's a comma separated string of parameter types
    pub param_types: Option<String>,
    pub description: Option<String>,
}

impl From<PyRemoteFunction> for wren_core::mdl::function::RemoteFunction {
    fn from(
        remote_function: PyRemoteFunction,
    ) -> wren_core::mdl::function::RemoteFunction {
        let param_names = remote_function.param_names.map(|names| {
            names
                .split(",")
                .map(|name| {
                    if name.is_empty() {
                        None
                    } else {
                        Some(name.to_string())
                    }
                })
                .collect::<Vec<Option<String>>>()
        });
        let param_types = remote_function.param_types.map(|types| {
            types
                .split(",")
                .map(|t| {
                    if t.is_empty() {
                        None
                    } else {
                        Some(t.to_string())
                    }
                })
                .collect::<Vec<Option<String>>>()
        });
        wren_core::mdl::function::RemoteFunction {
            function_type: FunctionType::from_str(&remote_function.function_type)
                .unwrap(),
            name: remote_function.name,
            return_type: remote_function.return_type.unwrap_or("string".to_string()),
            param_names,
            param_types,
            description: remote_function.description,
        }
    }
}
