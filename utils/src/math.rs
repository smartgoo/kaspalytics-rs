use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub fn percent_change(a: Decimal, b: Decimal, dp: u32) -> Option<Decimal> {
    if b == dec!(0) {
        return None;
    }

    let d = ((a - b) / b) * dec!(100);
    Some(d.round_dp(dp))
}
