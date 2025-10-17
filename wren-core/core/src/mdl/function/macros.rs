/// Creates a singleton `ScalarUDF` of the `$UDF` function and a function
/// named `$NAME` which returns that singleton.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
#[macro_export]
macro_rules! make_udf_function {
    ($UDF:ty, $NAME:ident) => {
        #[doc = concat!("Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ", stringify!($NAME))]
        pub fn $NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            // Singleton instance of the function
            static INSTANCE: std::sync::LazyLock<
                std::sync::Arc<datafusion::logical_expr::ScalarUDF>,
            > = std::sync::LazyLock::new(|| {
                std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                    <$UDF>::new(),
                ))
            });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}
