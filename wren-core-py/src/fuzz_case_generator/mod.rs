// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{collections::HashSet, sync::Arc};

use wren_core::datafusion::{
    arrow::datatypes::{IntervalUnit, TimeUnit},
    common::types::{LogicalField, NativeType},
    error::Result,
    logical_expr::{
        ArrayFunctionArgument, ArrayFunctionSignature, Coercion, ScalarUDF,
        TypeSignature, TypeSignatureClass,
    },
};

pub struct FuzzCaseGenerator {}

impl Default for FuzzCaseGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl FuzzCaseGenerator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn generate_scalar(&self, functions: &[Arc<ScalarUDF>]) -> Result<String> {
        let function_exprs = functions
            .iter()
            .map(|function| {
                let function_name = function.name();
                let signature = &function.signature().type_signature;
                self.generate_scalar_case(function_name, signature)
            })
            .collect::<Result<Vec<Vec<String>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("SELECT {}", function_exprs))
    }

    fn generate_scalar_case(
        &self,
        function_name: &str,
        signature: &TypeSignature,
    ) -> Result<Vec<String>> {
        let arguments = Self::generate_arguments(signature)?;
        Ok(arguments
            .iter()
            .map(|arg| {
                let arg = arg.clone();
                Ok(format!("{}({})", function_name, arg))
            })
            .collect::<Result<HashSet<String>>>()?
            .into_iter()
            .collect())
    }

    #[allow(clippy::redundant_closure)]
    fn generate_arguments(signature: &TypeSignature) -> Result<Vec<String>> {
        Ok(match signature {
            TypeSignature::Nullary => vec!["".to_string()],
            TypeSignature::Exact(types) => vec![types
                .iter()
                .map(|t| Self::generate_value_by_type(&NativeType::from(t)))
                .collect::<Result<Vec<String>>>()?
                .join(",")],
            TypeSignature::OneOf(types) => types
                .iter()
                .map(|t| Self::generate_arguments(t))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            TypeSignature::Any(size) => {
                let mut args = vec![];
                for _ in 0..*size {
                    args.push(Self::generate_value_by_type(&NativeType::String)?);
                }
                args
            }
            TypeSignature::ArraySignature(array_signature) => match array_signature {
                ArrayFunctionSignature::Array { arguments, .. } => arguments
                    .iter()
                    .map(|arg| match arg {
                        ArrayFunctionArgument::Array => Self::generate_value_by_type(
                            &NativeType::List(Arc::new(LogicalField {
                                name: "element".into(),
                                logical_type: Arc::new(NativeType::String),
                                nullable: true,
                            })),
                        ),
                        ArrayFunctionArgument::Element => {
                            Self::generate_value_by_type(&NativeType::String)
                        }
                        ArrayFunctionArgument::Index => {
                            Self::generate_value_by_type(&NativeType::Int32)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
                ArrayFunctionSignature::MapArray => vec!["{ 'key': 'value' }".into()],
                ArrayFunctionSignature::RecursiveArray => {
                    vec!["[[1,2],[2,4]]".into()]
                }
            },
            TypeSignature::Numeric(size) => {
                let mut args = vec![];
                for _ in 0..*size {
                    args.push(Self::generate_value_by_type(&NativeType::Int32)?);
                }
                args
            }
            TypeSignature::String(size) => {
                let mut args = vec![];
                for _ in 0..*size {
                    args.push(Self::generate_value_by_type(&NativeType::String)?);
                }
                args
            }
            TypeSignature::Uniform(size, types) => types
                .iter()
                .map(|t| {
                    let mut args = vec![];
                    for _ in 0..*size {
                        args.push(Self::generate_value_by_type(&NativeType::from(t))?);
                    }
                    Ok(args)
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            TypeSignature::Variadic(types) => types
                .iter()
                .map(|t| {
                    let mut args = vec![];
                    for _ in 0..2 {
                        args.push(Self::generate_value_by_type(&NativeType::from(t))?);
                    }
                    Ok(args)
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            TypeSignature::VariadicAny => {
                let mut args = vec![];
                for _ in 0..2 {
                    args.push(Self::generate_value_by_type(&NativeType::String)?);
                }
                args
            }
            TypeSignature::Coercible(coercions) => coercions
                .iter()
                .map(|t| match t {
                    Coercion::Exact { desired_type, .. }
                    | Coercion::Implicit { desired_type, .. } => {
                        Self::generate_value_by_logical_type(desired_type)
                    }
                })
                .collect::<Result<Vec<_>>>()?,
            TypeSignature::Comparable(size) => {
                let mut args = vec![];
                for _ in 0..*size {
                    args.push(Self::generate_value_by_type(&NativeType::Int32)?);
                }
                args
            }
            TypeSignature::UserDefined => vec![],
        })
    }

    fn generate_value_by_type(t: &NativeType) -> Result<String> {
        let value = match t {
            NativeType::Null => "null",
            NativeType::Boolean => "true",
            NativeType::Int8
            | NativeType::Int16
            | NativeType::Int32
            | NativeType::Int64
            | NativeType::UInt8
            | NativeType::UInt16
            | NativeType::UInt32
            | NativeType::UInt64 => "123",
            NativeType::Float16 | NativeType::Float32 | NativeType::Float64 => "123.45",
            NativeType::Timestamp(_, _) => "2023-01-01 00:00:00",
            NativeType::Date => "2023-01-01",
            NativeType::Time(_) => "12:34:56",
            NativeType::Duration(_) => "1 hour",
            NativeType::Interval(_) => "1 day",
            NativeType::FixedSizeBinary(_) | NativeType::Binary => "'123'",
            NativeType::String => "'Hello, world!'",
            NativeType::Decimal(_, _) => "123.45",
            NativeType::FixedSizeList(field, _) | NativeType::List(field) => {
                let inner_type = field.logical_type.native();
                let inner_value = Self::generate_value_by_type(inner_type)?;
                &format!("[{}]", inner_value)
            }
            NativeType::Struct(_) => "{ 'field1': 1, 'field2': 'value' }",
            NativeType::Map(_) => "{ 'key': 'value' }",
            NativeType::Union(_) => unimplemented!(),
        };
        Ok(value.to_string())
    }

    fn generate_value_by_logical_type(signature: &TypeSignatureClass) -> Result<String> {
        let native_type = match signature {
            TypeSignatureClass::Native(l) => l.native(),
            TypeSignatureClass::Timestamp => {
                &NativeType::Timestamp(TimeUnit::Nanosecond, None)
            }
            TypeSignatureClass::Time => &NativeType::Time(TimeUnit::Nanosecond),
            TypeSignatureClass::Interval => &NativeType::Interval(IntervalUnit::DayTime),
            TypeSignatureClass::Duration => &NativeType::Duration(TimeUnit::Nanosecond),
            TypeSignatureClass::Integer => &NativeType::Int32,
        };
        Self::generate_value_by_type(native_type)
    }
}
