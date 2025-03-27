use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::internal_err;
use datafusion::common::Result;
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, DocSection, Documentation,
    DocumentationBuilder, PartitionEvaluator, ScalarUDFImpl, Signature, TypeSignature,
    Volatility, WindowUDFImpl,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Display;
use std::str::FromStr;

use crate::logical_plan::utils::map_data_type;

#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
pub struct RemoteFunction {
    pub function_type: FunctionType,
    pub name: String,
    pub return_type: String,
    pub param_names: Option<Vec<String>>,
    pub param_types: Option<Vec<String>>,
    pub description: Option<String>,
}

impl RemoteFunction {
    pub fn get_signature(&self) -> Signature {
        let mut signatures = vec![];
        if let Some(param_types) = &self.param_types {
            if let Some(types) = Self::transform_param_type(param_types.as_slice()) {
                signatures.push(TypeSignature::Exact(types));
            }
        }
        // If the function has no siganture, we will add two default signatures: nullary and variadic any
        if signatures.is_empty() {
            signatures.push(TypeSignature::Nullary);
            signatures.push(TypeSignature::VariadicAny);
        }
        Signature::one_of(signatures, Volatility::Volatile)
    }

    fn transform_param_type(param_types: &[String]) -> Option<Vec<DataType>> {
        let types = param_types
            .iter()
            .map(|t| map_data_type(t.as_str()).ok())
            .collect::<Vec<_>>();
        if types.iter().any(|x| x.is_none()) {
            return None;
        }
        Some(types.into_iter().map(|x| x.unwrap().clone()).collect())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FunctionType {
    Scalar,
    Aggregate,
    Window,
}

impl Display for FunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            FunctionType::Scalar => "scalar".to_string(),
            FunctionType::Aggregate => "aggregate".to_string(),
            FunctionType::Window => "window".to_string(),
        };
        write!(f, "{}", str)
    }
}

impl FromStr for FunctionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "scalar" => Ok(FunctionType::Scalar),
            "aggregate" => Ok(FunctionType::Aggregate),
            "window" => Ok(FunctionType::Window),
            _ => Err(format!("Unknown function type: {}", s)),
        }
    }
}

/// A scalar UDF that will be bypassed when planning logical plan.
/// This is used to register the remote function to the context. The function should not be
/// invoked by DataFusion. It's only used to generate the logical plan and unparsed them to SQL.
#[derive(Debug)]
pub struct ByPassScalarUDF {
    name: String,
    return_type: DataType,
    signature: Signature,
    doc: Option<Documentation>,
}

impl ByPassScalarUDF {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Volatile,
            ),
            doc: None,
        }
    }
}

impl From<RemoteFunction> for ByPassScalarUDF {
    fn from(func: RemoteFunction) -> Self {
        let return_type = map_data_type(func.return_type.as_str()).unwrap();
        let mut builder = DocumentationBuilder::new_with_details(
            DocSection::default(),
            func.description.clone().unwrap_or("".to_string()),
            "",
        );
        let signature = func.get_signature();
        if let Some(param_names) = func.param_names.as_ref() {
            for (i, name) in param_names.iter().enumerate() {
                builder = builder
                    .with_argument(name, func.param_types.as_ref().unwrap()[i].as_str());
            }
        }
        ByPassScalarUDF {
            name: func.name,
            return_type,
            signature,
            doc: Some(builder.build()),
        }
    }
}

impl ScalarUDFImpl for ByPassScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!("This function should not be called")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc.as_ref()
    }
}

/// An aggregate UDF that will be bypassed when planning logical plan.
/// See [ByPassScalarUDF] for more details.
#[derive(Debug)]
pub struct ByPassAggregateUDF {
    name: String,
    return_type: DataType,
    signature: Signature,
    doc: Option<Documentation>,
}

impl ByPassAggregateUDF {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Volatile,
            ),
            doc: None,
        }
    }
}

impl From<RemoteFunction> for ByPassAggregateUDF {
    fn from(func: RemoteFunction) -> Self {
        let return_type = map_data_type(func.return_type.as_str()).unwrap();
        let mut builder = DocumentationBuilder::new_with_details(
            DocSection::default(),
            func.description.clone().unwrap_or("".to_string()),
            "",
        );
        let signature = func.get_signature();
        if let Some(param_names) = func.param_names.as_ref() {
            for (i, name) in param_names.iter().enumerate() {
                builder = builder
                    .with_argument(name, func.param_types.as_ref().unwrap()[i].as_str());
            }
        }

        ByPassAggregateUDF {
            name: func.name,
            return_type,
            signature,
            doc: Some(builder.build()),
        }
    }
}

impl AggregateUDFImpl for ByPassAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        internal_err!("This function should not be called")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc.as_ref()
    }
}

/// A window UDF that will be bypassed when planning logical plan.
/// See [ByPassScalarUDF] for more details.
#[derive(Debug)]
pub struct ByPassWindowFunction {
    name: String,
    return_type: DataType,
    signature: Signature,
    doc: Option<Documentation>,
}

impl ByPassWindowFunction {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Volatile,
            ),
            doc: None,
        }
    }
}

impl From<RemoteFunction> for ByPassWindowFunction {
    fn from(func: RemoteFunction) -> Self {
        let return_type = map_data_type(func.return_type.as_str()).unwrap();
        let mut builder = DocumentationBuilder::new_with_details(
            DocSection::default(),
            func.description.clone().unwrap_or("".to_string()),
            "",
        );
        let signature = func.get_signature();
        if let Some(param_names) = func.param_names.as_ref() {
            for (i, name) in param_names.iter().enumerate() {
                builder = builder
                    .with_argument(name, func.param_types.as_ref().unwrap()[i].as_str());
            }
        }

        ByPassWindowFunction {
            name: func.name,
            return_type,
            signature,
            doc: Some(builder.build()),
        }
    }
}

impl WindowUDFImpl for ByPassWindowFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        internal_err!("This function should not be called")
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        Ok(Field::new(
            field_args.name(),
            self.return_type.clone(),
            false,
        ))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc.as_ref()
    }
}

#[cfg(test)]
mod test {
    use crate::mdl::function::{
        ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction,
    };
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::Result;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_by_pass_scalar_udf() -> Result<()> {
        let udf = ByPassScalarUDF::new("date_diff", DataType::Int64);
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::new_from_impl(udf));

        let plan = ctx
            .sql("SELECT date_diff(1, 2)")
            .await?
            .into_unoptimized_plan();
        let expected = "Projection: date_diff(Int64(1), Int64(2))\n  EmptyRelation";
        assert_eq!(format!("{plan}"), expected);

        ctx.register_udf(ScalarUDF::new_from_impl(ByPassScalarUDF::new(
            "today",
            DataType::Utf8,
        )));
        let plan_2 = ctx.sql("SELECT today()").await?.into_unoptimized_plan();
        assert_eq!(format!("{plan_2}"), "Projection: today()\n  EmptyRelation");

        Ok(())
    }

    #[tokio::test]
    async fn test_by_pass_agg_udf() -> Result<()> {
        let udf = ByPassAggregateUDF::new("count_self", DataType::Int64);
        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(udf));

        let plan = ctx.sql("SELECT c2, count_self(*) FROM (VALUES (1,2), (2,3), (3,4)) t(c1, c2) GROUP BY 1").await?.into_unoptimized_plan();
        let expected = "Projection: t.c2, count_self(*)\
        \n  Aggregate: groupBy=[[t.c2]], aggr=[[count_self(*)]]\
        \n    SubqueryAlias: t\
        \n      Projection: column1 AS c1, column2 AS c2\
        \n        Values: (Int64(1), Int64(2)), (Int64(2), Int64(3)), (Int64(3), Int64(4))";
        assert_eq!(format!("{plan}"), expected);

        ctx.register_udaf(AggregateUDF::new_from_impl(ByPassAggregateUDF::new(
            "total_count",
            DataType::Int64,
        )));
        let plan_2 = ctx
            .sql("SELECT total_count() AS total_count FROM (VALUES (1), (2), (3)) AS val(x)")
            .await?
            .into_unoptimized_plan();
        assert_eq!(
            format!("{plan_2}"),
            "Projection: total_count() AS total_count\
        \n  Aggregate: groupBy=[[]], aggr=[[total_count()]]\
        \n    SubqueryAlias: val\n      Projection: column1 AS x\
        \n        Values: (Int64(1)), (Int64(2)), (Int64(3))"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_by_pass_window_udf() -> Result<()> {
        let udf = ByPassWindowFunction::new("custom_window", DataType::Int64);
        let ctx = SessionContext::new();
        ctx.register_udwf(WindowUDF::new_from_impl(udf));

        let plan = ctx
            .sql("SELECT custom_window(1, 2) OVER ()")
            .await?
            .into_unoptimized_plan();
        let expected = "Projection: custom_window(Int64(1),Int64(2)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[custom_window(Int64(1), Int64(2)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    EmptyRelation";
        assert_eq!(format!("{plan}"), expected);

        ctx.register_udwf(WindowUDF::new_from_impl(ByPassWindowFunction::new(
            "cume_dist",
            DataType::Int64,
        )));
        let plan_2 = ctx
            .sql("SELECT cume_dist() OVER ()")
            .await?
            .into_unoptimized_plan();
        assert_eq!(format!("{plan_2}"), "Projection: cume_dist() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[cume_dist() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    EmptyRelation");

        Ok(())
    }
}
