use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::common::Result;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, PartitionEvaluator, ScalarUDFImpl,
    Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use std::any::Any;

/// A scalar UDF that will be bypassed when planning logical plan.
/// This is used to register the remote function to the context. The function should not be
/// invoked by DataFusion. It's only used to generate the logical plan and unparsed them to SQL.
#[derive(Debug)]
pub struct ByPassScalarUDF {
    name: String,
    return_type: DataType,
    signature: Signature,
}

impl ByPassScalarUDF {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::one_of(
                vec![
                    TypeSignature::VariadicAny,
                    TypeSignature::Uniform(0, vec![]),
                ],
                Volatility::Volatile,
            ),
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
}

/// An aggregate UDF that will be bypassed when planning logical plan.
/// See [ByPassScalarUDF] for more details.
#[derive(Debug)]
pub struct ByPassAggregateUDF {
    name: String,
    return_type: DataType,
    signature: Signature,
}

impl ByPassAggregateUDF {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::variadic_any(Volatility::Immutable),
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
}

/// A window UDF that will be bypassed when planning logical plan.
/// See [ByPassScalarUDF] for more details.
#[derive(Debug)]
pub struct ByPassWindowFunction {
    name: String,
    return_type: DataType,
    signature: Signature,
}

impl ByPassWindowFunction {
    pub fn new(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature: Signature::variadic_any(Volatility::Immutable),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        internal_err!("This function should not be called")
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
        Ok(())
    }
}
