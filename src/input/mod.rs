pub mod binary;
pub mod dotgraph;
pub mod edgelist;

pub use binary::BinaryInput;
pub use dotgraph::DotGraph;
pub use dotgraph::DotGraphInput;
pub use edgelist::EdgeList;
pub use edgelist::EdgeListInput;

use crate::index::Idx;
use gdl::CypherValue;

pub struct InputPath<P>(pub(crate) P);

pub trait InputCapabilities<NI: Idx> {
    type GraphInput;
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

pub trait ParseValue: Default + Sized {
    fn parse(bytes: &[u8]) -> (Self, usize);
}

impl ParseValue for () {
    fn parse(_bytes: &[u8]) -> (Self, usize) {
        ((), 0)
    }
}

macro_rules! impl_parse_value {
    ($atoi:path, $($ty:ty),+ $(,)?) => {
        $(
            impl $crate::input::ParseValue for $ty {
                fn parse(bytes: &[u8]) -> (Self, usize) {
                    if bytes.len() == 0 {
                        (<$ty as ::std::default::Default>::default(), 0)
                    } else {
                        $atoi(bytes)
                    }
                }
            }
        )+
    };
}

impl_parse_value!(
    ::atoi::FromRadix10::from_radix_10,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
);

impl_parse_value!(
    ::atoi::FromRadix10Signed::from_radix_10_signed,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
);

impl_parse_value!(parse_float, f32, f64);

fn parse_float<T: fast_float::FastFloat>(bytes: &[u8]) -> (T, usize) {
    fast_float::parse_partial(bytes).unwrap()
}

pub struct MyCypherValue<'a>(&'a CypherValue);

impl<'a> From<MyCypherValue<'a>> for () {
    fn from(_: MyCypherValue) -> Self {}
}

macro_rules! impl_from_cypher_value {
    ($enum:path, $ty:ty) => {
        impl<'a> ::std::convert::From<$crate::input::MyCypherValue<'a>> for $ty {
            fn from(cv: $crate::input::MyCypherValue) -> Self {
                if let $enum(f) = cv.0 {
                    *f as $ty
                } else {
                    panic!("expected {} value", stringify!($ty))
                }
            }
        }
    };
}

impl_from_cypher_value!(CypherValue::Float, f32);
impl_from_cypher_value!(CypherValue::Float, f64);
impl_from_cypher_value!(CypherValue::Integer, i32);
impl_from_cypher_value!(CypherValue::Integer, i64);
