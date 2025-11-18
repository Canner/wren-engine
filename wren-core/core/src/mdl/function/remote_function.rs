use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion::common::{internal_err, not_impl_err};
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, DocSection, Documentation,
    DocumentationBuilder, PartitionEvaluator, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, Volatility, WindowUDFImpl,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use crate::logical_plan::utils::{get_coercion_type_signature, map_data_type};

#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
pub struct RemoteFunction {
    pub function_type: FunctionType,
    pub name: String,
    pub return_type: String,
    pub param_names: Option<Vec<Option<String>>>,
    pub param_types: Option<Vec<Option<String>>>,
    pub description: Option<String>,
}

impl RemoteFunction {
    pub fn get_signature(&self) -> Signature {
        let mut signatures = vec![];
        if let Some(param_types) = &self.param_types {
            if let Some(types) = Self::transform_param_type(param_types.as_slice()) {
                let coercions = types
                    .iter()
                    .map(get_coercion_type_signature)
                    .collect::<Vec<_>>();
                if coercions.iter().any(|r| r.is_err()) {
                    signatures.push(TypeSignature::Exact(types.clone()));
                } else {
                    let coercions = coercions.into_iter().map(|r| r.unwrap()).collect();
                    signatures.push(TypeSignature::Coercible(coercions));
                }
            }
        }
        // If the function has no siganture, we will add two default signatures: nullary and variadic any
        if signatures.is_empty() {
            signatures.push(TypeSignature::Nullary);
            signatures.push(TypeSignature::VariadicAny);
        }
        Signature::one_of(signatures, Volatility::Volatile)
    }

    fn transform_param_type(param_types: &[Option<String>]) -> Option<Vec<DataType>> {
        let types = param_types
            .iter()
            .map(|t| t.clone().and_then(|x| map_data_type(x.as_str()).ok()))
            .collect::<Vec<_>>();
        if types.iter().any(|x| x.is_none()) {
            return None;
        }
        Some(types.into_iter().flatten().collect())
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
        write!(f, "{str}")
    }
}

impl FromStr for FunctionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "scalar" => Ok(FunctionType::Scalar),
            "aggregate" => Ok(FunctionType::Aggregate),
            "window" => Ok(FunctionType::Window),
            _ => Err(format!("Unknown function type: {s}")),
        }
    }
}

/// The return type of the function.
/// It can be a specific data type, the same as the input type, or the same as the input array element type.
///
/// The return type is used to generate the logical plan and unparsed them to SQL.
/// It should not be used to check the return type of the function execution.
#[derive(Debug)]
pub enum ReturnType {
    /// The return type is a specific data type
    Specific(DataType),
    /// The return type is the same as the input type
    SameAsInput,
    /// If the input type is array, the return type is the same as the element type of the first array argument
    /// e.g. `greatest(array<int>)` will return `int`
    SameAsInputFirstArrayElement,
    /// The return type is the array of the first argument type
    ArrayOfInputFirstArgument,
}

impl Display for ReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReturnType::Specific(data_type) => write!(f, "{data_type}"),
            ReturnType::SameAsInput => write!(f, "same_as_input"),
            ReturnType::SameAsInputFirstArrayElement => {
                write!(f, "same_as_input_first_array_element")
            }
            ReturnType::ArrayOfInputFirstArgument => {
                write!(f, "array_of_input_first_argument")
            }
        }
    }
}
impl FromStr for ReturnType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "same_as_input" => Ok(ReturnType::SameAsInput),
            "same_as_input_first_array_element" => {
                Ok(ReturnType::SameAsInputFirstArrayElement)
            }
            "array_of_input_first_argument" => Ok(ReturnType::ArrayOfInputFirstArgument),
            _ => map_data_type(s)
                .map(ReturnType::Specific)
                .map_err(|e| e.to_string()),
        }
    }
}

impl ReturnType {
    pub fn to_data_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self {
            ReturnType::Specific(data_type) => data_type.clone(),
            ReturnType::SameAsInput => {
                arg_types.first().cloned().unwrap_or(DataType::Null)
            }
            ReturnType::SameAsInputFirstArrayElement => {
                if arg_types.is_empty() {
                    return not_impl_err!("No input type");
                }
                if let DataType::List(field) = &arg_types[0] {
                    field.data_type().clone()
                } else {
                    return not_impl_err!("Input type is not array");
                }
            }
            ReturnType::ArrayOfInputFirstArgument => {
                if arg_types.is_empty() {
                    return not_impl_err!("No input type");
                }
                DataType::List(Arc::new(Field::new("item", arg_types[0].clone(), true)))
            }
        })
    }
}

/// A scalar UDF that will be bypassed when planning logical plan.
/// This is used to register the remote function to the context. The function should not be
/// invoked by DataFusion. It's only used to generate the logical plan and unparsed them to SQL.
#[derive(Debug)]
pub struct ByPassScalarUDF {
    name: String,
    return_type: ReturnType,
    signature: Signature,
    doc: Option<Documentation>,
}

impl ByPassScalarUDF {
    pub fn new(
        name: &str,
        return_type: ReturnType,
        signature: Signature,
        doc: Option<Documentation>,
    ) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature,
            doc,
        }
    }

    pub fn new_with_return_type(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type: ReturnType::Specific(return_type),
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
        // just panic if the return type is not valid to avoid we input invalid type
        let return_type = ReturnType::from_str(&func.return_type)
            .unwrap_or(ReturnType::Specific(DataType::Utf8));
        ByPassScalarUDF {
            return_type,
            signature: func.get_signature(),
            doc: Some(build_document(&func)),
            name: func.name,
        }
    }
}

fn build_document(func: &RemoteFunction) -> Documentation {
    let mut builder = DocumentationBuilder::new_with_details(
        DocSection::default(),
        func.description.clone().unwrap_or("".to_string()),
        "",
    );
    if let Some(param_names) = func.param_names.as_ref() {
        for (i, name) in param_names.iter().enumerate() {
            let description = func
                .param_types
                .as_ref()
                .map(|types| types[i].clone().unwrap_or("".to_string()))
                .unwrap_or("".to_string());
            builder = builder
                .with_argument(name.clone().unwrap_or("".to_string()), description);
        }
    }
    builder.build()
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.return_type.to_data_type(arg_types)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
    return_type: ReturnType,
    signature: Signature,
    aliases: Vec<String>,
    doc: Option<Documentation>,
}

impl ByPassAggregateUDF {
    pub fn new(
        name: &str,
        return_type: ReturnType,
        signature: Signature,
        doc: Option<Documentation>,
    ) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature,
            aliases: vec![],
            doc,
        }
    }

    pub fn new_with_alias(
        name: &str,
        return_type: ReturnType,
        signature: Signature,
        aliases: Vec<String>,
        doc: Option<Documentation>,
    ) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature,
            aliases,
            doc,
        }
    }

    pub fn new_with_return_type(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type: ReturnType::Specific(return_type),
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Volatile,
            ),
            aliases: vec![],
            doc: None,
        }
    }
}

impl From<RemoteFunction> for ByPassAggregateUDF {
    fn from(func: RemoteFunction) -> Self {
        // just panic if the return type is not valid to avoid we input invalid type
        let return_type = ReturnType::from_str(&func.return_type).unwrap();
        ByPassAggregateUDF {
            return_type,
            signature: func.get_signature(),
            doc: Some(build_document(&func)),
            name: func.name,
            aliases: vec![],
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

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.return_type.to_data_type(arg_types)
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
    return_type: ReturnType,
    signature: Signature,
    doc: Option<Documentation>,
}

impl ByPassWindowFunction {
    pub fn new(
        name: &str,
        return_type: ReturnType,
        signature: Signature,
        doc: Option<Documentation>,
    ) -> Self {
        Self {
            name: name.to_string(),
            return_type,
            signature,
            doc,
        }
    }

    pub fn new_with_return_type(name: &str, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            return_type: ReturnType::Specific(return_type),
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
        // just panic if the return type is not valid to avoid we input invalid type
        let return_type = ReturnType::from_str(&func.return_type).unwrap();
        ByPassWindowFunction {
            return_type,
            signature: func.get_signature(),
            doc: Some(build_document(&func)),
            name: func.name,
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

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = self.return_type.to_data_type(
            &field_args
                .input_fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Field::new(field_args.name(), return_type, false).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc.as_ref()
    }
}

#[cfg(test)]
mod test {
    use std::slice::from_ref;
    use std::sync::Arc;

    use crate::mdl::function::{
        ByPassAggregateUDF, ByPassScalarUDF, ByPassWindowFunction, FunctionType,
        RemoteFunction,
    };
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::types::logical_string;
    use datafusion::common::Result;
    use datafusion::logical_expr::TypeSignatureClass;
    use datafusion::logical_expr::{AggregateUDF, ScalarUDF, ScalarUDFImpl, WindowUDF};
    use datafusion::logical_expr::{Coercion, TypeSignature};
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_by_pass_scalar_udf() -> Result<()> {
        let udf = ByPassScalarUDF::new_with_return_type("date_test", DataType::Int64);
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::new_from_impl(udf));

        let plan = ctx
            .sql("SELECT date_test(1, 2)")
            .await?
            .into_unoptimized_plan();
        let expected = "Projection: date_test(Int64(1), Int64(2))\n  EmptyRelation";
        assert_eq!(format!("{plan}"), expected);

        ctx.register_udf(ScalarUDF::new_from_impl(
            ByPassScalarUDF::new_with_return_type("today", DataType::Utf8),
        ));
        let plan_2 = ctx.sql("SELECT today()").await?.into_unoptimized_plan();
        assert_eq!(format!("{plan_2}"), "Projection: today()\n  EmptyRelation");

        Ok(())
    }

    #[tokio::test]
    async fn test_by_pass_agg_udf() -> Result<()> {
        let udf = ByPassAggregateUDF::new_with_return_type("count_self", DataType::Int64);
        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::new_from_impl(udf));

        let plan = ctx.sql("SELECT c2, count_self(*) FROM (VALUES (1,2), (2,3), (3,4)) t(c1, c2) GROUP BY 1").await?.into_unoptimized_plan();
        let expected = "Projection: t.c2, count_self(*)\
        \n  Aggregate: groupBy=[[t.c2]], aggr=[[count_self(*)]]\
        \n    SubqueryAlias: t\
        \n      Projection: column1 AS c1, column2 AS c2\
        \n        Values: (Int64(1), Int64(2)), (Int64(2), Int64(3)), (Int64(3), Int64(4))";
        assert_eq!(format!("{plan}"), expected);

        ctx.register_udaf(AggregateUDF::new_from_impl(
            ByPassAggregateUDF::new_with_return_type("total_count", DataType::Int64),
        ));
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
        let udf =
            ByPassWindowFunction::new_with_return_type("custom_window", DataType::Int64);
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

        ctx.register_udwf(WindowUDF::new_from_impl(
            ByPassWindowFunction::new_with_return_type("cume_dist", DataType::Int64),
        ));
        let plan_2 = ctx
            .sql("SELECT cume_dist() OVER ()")
            .await?
            .into_unoptimized_plan();
        assert_eq!(format!("{plan_2}"), "Projection: cume_dist() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[cume_dist() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    EmptyRelation");

        Ok(())
    }

    #[tokio::test]
    async fn test_remote_function_to_bypass_func() -> Result<()> {
        // full information
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "string".to_string(),
            param_names: Some(vec![Some("a".to_string()), Some("b".to_string())]),
            param_types: Some(vec![Some("int".to_string()), Some("string".to_string())]),
            description: Some("test function".to_string()),
        };
        let udf = ByPassScalarUDF::from(remote_function);
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(&[DataType::Int64]).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![TypeSignature::Coercible(vec![
                Coercion::new_exact(TypeSignatureClass::Integer),
                Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
            ])])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");
        assert_eq!(
            doc.arguments.unwrap(),
            vec![
                ("a".to_string(), "int".to_string()),
                ("b".to_string(), "string".to_string()),
            ]
        );

        // missing param names
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "string".to_string(),
            param_names: None,
            param_types: Some(vec![Some("int".to_string()), Some("string".to_string())]),
            description: Some("test function".to_string()),
        };

        let udf = ByPassScalarUDF::from(remote_function);
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(&[DataType::Int64]).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![TypeSignature::Coercible(vec![
                Coercion::new_exact(TypeSignatureClass::Integer),
                Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
            ])])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");
        assert_eq!(doc.arguments, None);

        // missing param types
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "string".to_string(),
            param_names: Some(vec![Some("a".to_string()), Some("b".to_string())]),
            param_types: None,
            description: Some("test function".to_string()),
        };

        let udf = ByPassScalarUDF::from(remote_function);
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(&[DataType::Int64]).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![
                TypeSignature::Nullary,
                TypeSignature::VariadicAny
            ])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");
        assert_eq!(
            doc.arguments.unwrap(),
            vec![
                ("a".to_string(), "".to_string()),
                ("b".to_string(), "".to_string()),
            ]
        );

        // same as input
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "same_as_input".to_string(),
            param_names: Some(vec![Some("a".to_string())]),
            param_types: Some(vec![Some("int".to_string())]),
            description: Some("test function".to_string()),
        };
        let udf = ByPassScalarUDF::from(remote_function);
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(&[DataType::Int64]).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![TypeSignature::Coercible(vec![
                Coercion::new_exact(TypeSignatureClass::Integer)
            ])])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");
        assert_eq!(
            doc.arguments.unwrap(),
            vec![("a".to_string(), "int".to_string()),]
        );

        // same as input first array element
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "same_as_input_first_array_element".to_string(),
            param_names: Some(vec![Some("a".to_string())]),
            param_types: Some(vec![Some("array<int>".to_string())]),
            description: Some("test function".to_string()),
        };
        let udf = ByPassScalarUDF::from(remote_function);
        let list_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(from_ref(&list_type)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![TypeSignature::Exact(vec![list_type.clone()])])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");

        // same as input missing param types
        let remote_function = RemoteFunction {
            function_type: FunctionType::Scalar,
            name: "test".to_string(),
            return_type: "same_as_input".to_string(),
            param_names: Some(vec![Some("a".to_string())]),
            param_types: None,
            description: Some("test function".to_string()),
        };
        let udf = ByPassScalarUDF::from(remote_function);
        assert_eq!(udf.name, "test");
        assert_eq!(
            udf.return_type.to_data_type(&[DataType::Int64]).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            udf.signature.type_signature,
            TypeSignature::OneOf(vec![
                TypeSignature::Nullary,
                TypeSignature::VariadicAny
            ])
        );
        let doc = udf.documentation().unwrap().clone();
        assert_eq!(doc.description, "test function");
        assert_eq!(
            doc.arguments.unwrap(),
            vec![("a".to_string(), "".to_string()),]
        );
        Ok(())
    }
}
