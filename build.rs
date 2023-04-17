fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(
            "AdvancedOrderKind",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            "AdvancedOrder",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .out_dir("lib/")
        .compile(
            &[
                "proto/advanced_order.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
