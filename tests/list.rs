use std::{
    array,
    panic::{self, AssertUnwindSafe},
    pin::{Pin, pin},
    sync::atomic::Ordering::Relaxed,
};

use linking::{EAGER, LAZY, LinkingMode, SERIALIZED};
use loom::{model, thread};
use maillon::{
    List, Node, NodeState,
    linking::{AtomicLazy, Linking, Serialized},
    list::{Back, End, Front, LIST_STATE_MAX, LockedList},
    node::{NodeData, NodeRef, NodeUnlinked},
};
use rstest::rstest;

mod linking;
mod loom;

type TestList<L> = List<TestData, (), (), L>;
type TestNode<'a, L> = Node<&'a TestList<L>>;
struct TestData(usize);
impl<'a, L: Linking> NodeData<&'a TestList<L>> for TestData {
    fn new_state_if_last_node_on_drop(
        self: Pin<&mut Self>,
        _list: &&'a TestList<L>,
        _list_data: &mut (),
    ) {
    }
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        _list: &'list &'a TestList<L>,
        _locked: Option<LockedList<'list, Self, (), (), L>>,
        _state_updated_on_unlink: bool,
    ) {
    }
}

fn push_node<L: Linking>(list: &TestList<L>, id: usize) -> Pin<Box<TestNode<'_, L>>> {
    let mut node = Box::pin(TestNode::with_data(list, TestData(id)));
    match node.as_mut().state() {
        NodeState::Unlinked(node) => node.push_back(Relaxed),
        NodeState::Linked(_) => unreachable!(),
    }
    node
}

#[cfg(not(skip_single_threaded))]
#[test]
#[should_panic(expected = "list state overflow")]
fn state_overflow() {
    List::<TestData, usize>::with_state(LIST_STATE_MAX + 1);
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn drop_non_empty_drain<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 2] = array::from_fn(|i| push_node(&list, i));
        drop(list.lock().drain());
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn panic_in_drain_execute_unlocked<L: Linking>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 2] = array::from_fn(|i| push_node(&list, i));
        {
            let drain = pin!(list.lock().drain());
            panic::catch_unwind(AssertUnwindSafe(|| drain.execute_unlocked(|| panic!())))
                .unwrap_err();
        }
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn remove_many<L: Linking, E: End>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
    #[values((Front, [0, 1, 2]), (Back, [2, 1, 0]))] (_end, ids): (E, [usize; 3]),
) {
    model(move || {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i));
        let mut locked = list.lock();
        let mut end = locked.end::<E>();
        for id in ids {
            let node = end.expect("cursor should reach every node");
            assert_eq!(node.data().0, id);
            assert!(nodes[id].is_linked());
            end = node.unlink();
            assert!(!nodes[id].is_linked());
        }
        assert!(end.is_none());
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn unlink_after_push<L: Linking, E: End>(
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
    #[values((Front, 1), (Back, 2))] (_end, next_id): (E, usize),
) {
    model(move || {
        let list = TestList::<L>::new();
        let mut nodes = vec![push_node(&list, 0)];
        let mut locked = list.lock();
        let end = locked.end::<E>().unwrap();
        nodes.push(push_node(&list, 1));
        nodes.push(push_node(&list, 2));
        let next = end.unlink().map(|next| next.data().0);
        assert_eq!(next, Some(next_id));
        assert!(!nodes[0].is_linked());
        assert_eq!(ids(&mut locked), [1, 2]);
        drop(locked);
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn drain_many<L: Linking, E: End>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
    #[values((Front, [0, 1, 2]), (Back, [2, 1, 0]))] (_end, ids): (E, [usize; 3]),
) {
    model(move || {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i));
        let mut drain = pin!(list.lock().drain());
        let mut end = drain.as_mut().end::<E>();
        assert!(list.is_empty(Relaxed));
        for id in ids {
            let node = end.expect("cursor should reach every node");
            assert_eq!(node.data().0, id);
            assert!(nodes[id].is_linked());
            end = node.unlink();
            assert!(!nodes[id].is_linked());
        }
        assert!(end.is_none());
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn cursor_empty<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        assert!(cursor.current().is_none());
        cursor.move_next();
        assert!(cursor.current().is_none());
        cursor.move_prev();
        assert!(cursor.current().is_none());
        assert!(!cursor.remove_current());
        let mut cursor = locked.cursor_back();
        assert!(cursor.current().is_none());
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn cursor_move<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let _nodes: [_; 3] = array::from_fn(|i| push_node(&list, i));
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        for id in [0, 1, 2] {
            assert_eq!(cursor.current().unwrap().0, id);
            cursor.move_next();
        }
        assert!(cursor.current().is_none());
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 0);
        cursor.move_prev();
        assert!(cursor.current().is_none());
        for id in [2, 1, 0] {
            cursor.move_prev();
            assert_eq!(cursor.current().unwrap().0, id);
        }
        cursor.move_prev();
        assert!(cursor.current().is_none());
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn cursor_remove_current<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i));
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        cursor.move_next();
        assert!(cursor.remove_current());
        assert!(!nodes[1].is_linked());
        assert_eq!(cursor.current().unwrap().0, 2);
        assert!(cursor.remove_current());
        assert!(!nodes[2].is_linked());
        assert!(cursor.current().is_none());
        assert!(!cursor.remove_current());
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 0);
        assert!(cursor.remove_current());
        assert!(cursor.current().is_none());
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn cursor_remove_concurrent_push<L: Linking>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = TestList::<L>::new();
        let node = push_node(&list, 0);
        thread::scope(|scope| {
            scope.spawn(|| drop(push_node(&list, 1)));
            scope.spawn(|| list.lock().cursor_back().remove_current());
        });
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        while cursor.remove_current() {}
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(!node.is_linked());
    });
}

fn ids<L: Linking>(locked: &mut LockedList<'_, TestData, (), (), L>) -> Vec<usize> {
    let mut ids = Vec::new();
    let mut cursor = locked.cursor_front();
    while let Some(current) = cursor.current() {
        ids.push(current.0);
        cursor.move_next();
    }
    ids
}

#[cfg(not(skip_single_threaded))]
#[test]
fn locked_push_back() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut nodes: [_; 3] =
            array::from_fn(|i| Box::pin(TestNode::with_data(&list, TestData(i))));
        let mut locked = list.lock();
        for node in &mut nodes {
            match node.as_mut().state() {
                NodeState::Unlinked(node) => locked.push_back(node, Relaxed),
                NodeState::Linked(_) => unreachable!(),
            }
            assert!(node.is_linked());
        }
        assert_eq!(ids(&mut locked), [0, 1, 2]);
        let mut cursor = locked.cursor_front();
        while cursor.remove_current() {}
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[test]
fn cursor_insert() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut nodes: [_; 6] =
            array::from_fn(|i| Box::pin(TestNode::with_data(&list, TestData(i))));
        let unlinked: fn(&mut Pin<Box<Node<_>>>) -> NodeUnlinked<'_, _> =
            |node| match node.as_mut().state() {
                NodeState::Unlinked(node) => node,
                NodeState::Linked(_) => unreachable!(),
            };
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        cursor.insert_after(unlinked(&mut nodes[0]), Relaxed);
        cursor.insert_after(unlinked(&mut nodes[1]), Relaxed);
        cursor.insert_before(unlinked(&mut nodes[2]), Relaxed);
        assert!(cursor.current().is_none());
        assert_eq!(ids(&mut locked), [1, 0, 2]);
        let mut cursor = locked.cursor_front();
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 0);
        cursor.insert_before(unlinked(&mut nodes[3]), Relaxed);
        cursor.insert_after(unlinked(&mut nodes[4]), Relaxed);
        assert_eq!(cursor.current().unwrap().0, 0);
        assert_eq!(ids(&mut locked), [1, 3, 0, 4, 2]);
        let mut cursor = locked.cursor_back();
        cursor.insert_after(unlinked(&mut nodes[5]), Relaxed);
        assert_eq!(ids(&mut locked), [1, 3, 0, 4, 2, 5]);
        let mut cursor = locked.cursor_back();
        assert_eq!(cursor.current().unwrap().0, 5);
        cursor.move_prev();
        assert_eq!(cursor.current().unwrap().0, 2);
        let mut cursor = locked.cursor_front();
        while cursor.remove_current() {}
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[cfg(not(skip_single_threaded))]
#[test]
fn locked_push_back_from_node_only() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut node = Box::pin(TestNode::with_data(&list, TestData(0)));
        let NodeState::Unlinked(unlinked) = node.as_mut().state() else {
            unreachable!()
        };
        let mut locked = maillon::list::ListRef::as_list(unlinked.list()).lock();
        locked.push_back(unlinked, Relaxed);
        drop(locked);
        assert!(node.is_linked());
    });
}

#[cfg(not(skip_single_threaded))]
#[rstest]
fn lazy_materialization(
    #[values(false, true)] head_materialized: bool,
    #[values(false, true)] cache_without_next: bool,
    #[values(false, true)] materialize_until_cached: bool,
    #[values(false, true)] unlink_cached: bool,
) {
    model(move || {
        let list = TestList::<AtomicLazy>::new();
        let mut nodes = (0..=4)
            .map(|i| Box::pin(TestNode::with_data(&list, TestData(i))))
            .collect::<Vec<_>>();
        let link = |node: &mut Pin<Box<TestNode<AtomicLazy>>>| match node.as_mut().state() {
            NodeState::Unlinked(node) => node.push_back(Relaxed),
            _ => unreachable!(),
        };
        let unlink = |node: &mut Pin<Box<TestNode<AtomicLazy>>>| match node.as_mut().state() {
            NodeState::Linked(node) => {
                node.unlink();
            }
            _ => unreachable!(),
        };
        link(&mut nodes[0]);
        link(&mut nodes[1]);
        if head_materialized {
            list.lock().front();
        }
        link(&mut nodes[2]);
        link(&mut nodes[3]);
        link(&mut nodes[4]);
        unlink(&mut nodes[3]);
        if cache_without_next {
            unlink(&mut nodes[4]);
            link(&mut nodes[4]);
        }
        nodes.push(Box::pin(TestNode::with_data(&list, TestData(5))));
        link(&mut nodes[5]);
        if materialize_until_cached {
            nodes.push(Box::pin(TestNode::with_data(&list, TestData(6))));
            link(&mut nodes[6]);
            unlink(&mut nodes[5]);
        }
        if unlink_cached {
            unlink(&mut nodes[2]);
        }
        {
            pin!(list.lock().drain()).front();
        }
        for node in &mut nodes {
            assert!(!node.is_linked());
        }
    });
}
