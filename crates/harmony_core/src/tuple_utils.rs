pub struct Here;
pub struct Later<T>(std::marker::PhantomData<T>);

pub trait HasTypeAt<Index, T> {}
impl<T, Tail> HasTypeAt<Here, T> for (T, Tail) {}
impl<T, U, I, Tail> HasTypeAt<Later<I>, T> for (U, Tail) where Tail: HasTypeAt<I, T> {}

pub trait AsNestedTuple<Tail> {
    type Nested;
}

impl<Tail> AsNestedTuple<Tail> for () {
    type Nested = ();
}
#[macro_export]
macro_rules! nested_tuple {
    (; $end:ty) => {
        $end
    };
    ($head:ty $(, $tail:ty)* ; $end:ty) => {
        ($head, nested_tuple!($($tail),*; $end))
    };
}

#[macro_export]
macro_rules! impl_as_nested_tuple_inner {
    ($($types:ident),+) => {
        impl<$($types),+, Tail> AsNestedTuple<Tail> for ($($types,)+) {
            type Nested = nested_tuple!($($types),*; Tail);
        }
    };
}

#[macro_export]
macro_rules! impl_as_nested_tuple {
    ($head:ident $(, $tail:ident)* $(,)?) => {
        impl_as_nested_tuple!(@impl $head $(, $tail)*);
        impl_as_nested_tuple!($($tail),*);
    };
    () => {
    };

    (@impl $($name:ident),+) => {
        impl_as_nested_tuple_inner!($($name),+);
     };
}
