/// A macro to define a simple wrapper around a [`Node`], handling [pin-projection].
///
/// # Example
///
/// ```
/// # use core::{future::Future, pin::Pin, task::{Context, Poll}};
/// use maillon::{List, Node, NodeState, node_wrapper};
///
/// struct MyNodeData {/* ... */}
///
/// # impl maillon::NodeData<&List<MyNodeData>> for MyNodeData {
/// #     fn new_state_if_last_node_on_drop(
/// #         self: Pin<&mut Self>,
/// #         _list: &&List<MyNodeData>,
/// #         _list_data: &mut (),
/// #     ) -> () {
/// #         todo!()
/// #     }
/// #     fn on_drop<'list>(
/// #         self: Pin<&mut Self>,
/// #         _list: &'list &List<MyNodeData>,
/// #         _locked: Option<maillon::LockedList<'list, MyNodeData>>,
/// #         _state_updated_on_unlink: bool,
/// #     ) {
/// #         todo!()
/// #     }
/// # }
/// #
/// node_wrapper! {
///     pub struct MyFuture<'a>(Node<&'a List<MyNodeData>>);
/// }
///
/// impl Future for MyFuture<'_> {
///     type Output = ();
///     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
///         // use `node_mut` to pin-project the node field
///         match self.node_mut().state() {
///             NodeState::Unlinked(node) => todo!(),
///             NodeState::Linked(node) => todo!(),
///         }
///     }
/// }
/// ```
///
/// This snippet roughly expands to the following code:
///
/// ```
/// # use core::{future::Future, pin::Pin, task::{Context, Poll}};
/// use maillon::{List, Node, NodeState, node_wrapper};
///
/// struct MyNodeData {/* ... */}
///
/// # impl maillon::NodeData<&List<MyNodeData>> for MyNodeData {
/// #     fn new_state_if_last_node_on_drop(
/// #         self: Pin<&mut Self>,
/// #         _list: &&List<MyNodeData>,
/// #         _list_data: &mut (),
/// #     ) -> () {
/// #         todo!()
/// #     }
/// #     fn on_drop<'list>(
/// #         self: Pin<&mut Self>,
/// #         _list: &'list &List<MyNodeData>,
/// #         _locked: Option<maillon::LockedList<'list, MyNodeData>>,
/// #         _state_updated_on_unlink: bool,
/// #     ) {
/// #         todo!()
/// #     }
/// # }
/// #
/// pub struct MyFuture<'a>(Node<&'a List<MyNodeData>>);
///
/// impl<'a> MyFuture<'a> {
///     fn node(&self) -> &Node<&'a List<MyNodeData>> {
///         /* ... */
/// #        todo!()
///     }
///
///     fn node_mut(self: Pin<&mut Self>) -> Pin<&mut Node<&'a List<MyNodeData>>> {
///         /* ... */
/// #        todo!()
///     }
/// }
///
/// impl Future for MyFuture<'_> {
///     type Output = ();
///     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
///         // use `node_mut` to pin-project the node field
///         match self.node_mut().state() {
///             NodeState::Unlinked(node) => todo!(),
///             NodeState::Linked(node) => todo!(),
///         }
///     }
/// }
/// ```
///
/// This macro is deliberately simple and only supports a single-field tuple-struct, with (almost)
/// arbitrary generic parameters. More complex use cases might require a dedicated crate like
/// [pin-project-lite].
///
/// [`Node`]: crate::Node
/// [pin-projection]: https://doc.rust-lang.org/std/pin/index.html#projections-and-structural-pinning
/// [pin-project-lite]: https://docs.rs/pin-project-lite/
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
        $vis:vis struct $name:ident ($node_vis:vis $node:ty)
        $(where $($where_ty:ty : $where_bound:path),* $(,)?)?;
    ) => {
        $crate::__node_wrapper! {
            $(#[$attr])*
            $vis struct $name<> ($node_vis $node)
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
        > ($node_vis:vis $node:ty)
        $(where $($where_ty:ty : $where_bound:path),* $(,)?)? ;
    ) => {
        $crate::__node_wrapper!(
            @generate [$($head)*] [$($def)*] [$($imp)*] [$($use)*] $node_vis $node
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
        [$($def:tt)*] [$($imp:tt)*] [$($use:tt)*] $node_vis:vis $node:ty [$($where:tt)*]
    ) => {
        $(#[$attr])*
        $vis struct $name<$($def)*>($node_vis $node) where $($where)*;

        impl<$($imp)*> $name<$($use)*> where $($where)* {
            #[inline(always)]
            $node_vis fn node(&self) -> &$node {
                &self.0
            }

            #[inline(always)]
            $node_vis fn node_mut(self: ::core::pin::Pin<&mut Self>) -> ::core::pin::Pin<&mut $node> {
                unsafe { ::core::pin::Pin::map_unchecked_mut(self, |this| &mut this.0) }
            }
        }

        const _: () = {
            #[allow(dead_code)]
            trait MustNotImplDrop {}
            #[allow(drop_bounds)]
            impl<T: ::core::ops::Drop + ?::core::marker::Sized> MustNotImplDrop for T {}
            impl<$($imp)*> MustNotImplDrop for $name<$($use)*> where $($where)* {}
        };
    };
}
