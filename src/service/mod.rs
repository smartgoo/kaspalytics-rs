pub mod analysis;
mod stats;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum Granularity {
    Second,
    Minute,
    Hour,
    Day,
}

impl std::fmt::Display for Granularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Granularity::Second => write!(f, "Second"),
            Granularity::Minute => write!(f, "Minute"),
            Granularity::Hour => write!(f, "Hour"),
            Granularity::Day => write!(f, "Day"),
        }
    }
}
