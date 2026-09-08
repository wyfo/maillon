#[macro_export]
macro_rules! node_wrapper {
    ($($tt:tt)*) => {
        $crate::__node_wrapper!($($tt)*);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __node_wrapper {
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident ($node:ty)
        $(where $($where_ty:ty : $where_bound:path),* $(,)?)?;
    ) => {
        $crate::__node_wrapper! {
            $(#[$attr])*
            $vis struct $name<> ($node)
            $(where $($where_ty : $where_bound,)*)?;
        }
    };
    (
        $(#[$attr:meta])*
        $vis:vis struct $name:ident < $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(@generics [$(#[$attr])* $vis $name] [] [] [] $($rest)*);
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        > ($node:ty)
        $(where $($where_ty:ty : $where_bound:path),* $(,)?)? ;
    ) => {
        $crate::__node_wrapper!(
            @generate [$($head)*] [$($def)*] [$($imp)*] [$($use)*] $node
            [$($($where_ty: $where_bound,)*)?]
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        $lt:lifetime $(: $bound:lifetime)? , $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* $lt $(: $bound)?,] [$($imp)* $lt $(: $bound)?,]
            [$($use)* $lt,] $($rest)*
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        $lt:lifetime $(: $bound:lifetime)? > $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* $lt $(: $bound)?,] [$($imp)* $lt $(: $bound)?,]
            [$($use)* $lt,] > $($rest)*
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        const $cst:ident : $cst_ty:ty $(= $default:tt)? , $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* const $cst: $cst_ty $(= $default)?,]
            [$($imp)* const $cst: $cst_ty,] [$($use)* $cst,] $($rest)*
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        const $cst:ident : $cst_ty:ty $(= $default:tt)? > $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* const $cst: $cst_ty $(= $default)?,]
            [$($imp)* const $cst: $cst_ty,] [$($use)* $cst,] > $($rest)*
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        $param:ident $(: $bound:path)? $(= $default:ty)? , $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* $param $(: $bound)? $(= $default)?,]
            [$($imp)* $param $(: $bound)?,] [$($use)* $param,] $($rest)*
        );
    };
    (
        @generics [$($head:tt)*] [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*]
        $param:ident $(: $bound:path)? $(= $default:ty)? > $($rest:tt)*
    ) => {
        $crate::__node_wrapper!(
            @generics [$($head)*] [$($def)* $param $(: $bound)? $(= $default)?,]
            [$($imp)* $param $(: $bound)?,] [$($use)* $param,] > $($rest)*
        );
    };
    (
        @generate [$(#[$attr:meta])* $vis:vis $name:ident]
        [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*] $node:ty [$($where:tt)*]
    ) => {
        $(#[$attr])*
        $vis struct $name<$($def)*>($node) where $($where)*;

        impl<$($imp)*> $name<$($use)*> where $($where)* {
            #[inline(always)]
            fn node(&self) -> &$node {
                &self.0
            }

            #[inline(always)]
            fn node_mut(self: ::core::pin::Pin<&mut Self>) -> ::core::pin::Pin<&mut $node> {
                unsafe { ::core::pin::Pin::map_unchecked_mut(self, |this| &mut this.0) }
            }
        }
    };
}
