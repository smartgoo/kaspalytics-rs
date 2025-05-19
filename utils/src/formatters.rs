pub fn hash_rate_with_unit(hash_rates: &[u64]) -> (Vec<f64>, String) {
    let units = [
        ("YH/s", 1e24),
        ("ZH/s", 1e21),
        ("EH/s", 1e18),
        ("PH/s", 1e15),
        ("TH/s", 1e12),
        ("GH/s", 1e9),
        ("MH/s", 1e6),
        ("kH/s", 1e3),
        ("H/s", 1.0),
    ];

    let max_hash_rate = *hash_rates.iter().max().unwrap_or(&0) as f64;

    let (unit, divisor) = units
        .iter()
        .find(|(_, d)| max_hash_rate >= *d)
        .unwrap_or(&("H/s", 1.0));

    let converted: Vec<f64> = hash_rates
        .iter()
        .map(|&rate| rate as f64 / *divisor)
        .collect();

    (converted, unit.to_string())
}
