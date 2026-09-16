use std::{
    array,
    panic::{self, AssertUnwindSafe},
    pin::{Pin, pin},
    sync::atomic::Ordering::Relaxed,
};

use aiq::{
    List, Node, NodeState,
    list::{
        DrainEnd, DrainGetEnd, GetBack, GetFront, LIST_STATE_MAX, Linking, ListEnd, ListGetEnd,
        LockedList, Serialized,
    },
    node::{NodeData, NodeRef},
};
use linking::{EAGER, LAZY, LinkingMode, SERIALIZED};
use loom::{model, thread};
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

#[test]
#[should_panic(expected = "list state overflow")]
fn state_overflow() {
    List::<TestData, usize>::with_state(LIST_STATE_MAX + 1);
}

#[rstest]
fn drop_non_empty_drain<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 2] = array::from_fn(|i| push_node(&list, i + 1));
        drop(list.lock().drain());
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn panic_in_drain_execute_unlocked<L: Linking>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 2] = array::from_fn(|i| push_node(&list, i + 1));
        {
            let drain = pin!(list.lock().drain());
            panic::catch_unwind(AssertUnwindSafe(|| drain.execute_unlocked(|| panic!())))
                .unwrap_err();
        }
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn remove_many<L: Linking, E: ListGetEnd>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
    #[values((GetFront, [1, 2, 3]), (GetBack, [3, 2, 1]))] (_end, ids): (E, [usize; 3]),
) {
    model(move || {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i + 1));
        let mut locked = list.lock();
        let mut end = E::get_end(&mut locked);
        for id in ids {
            let node = end.expect("cursor should reach every node");
            assert_eq!(node.data().0, id);
            assert!(nodes[id - 1].is_linked());
            end = node.unlink(|_, _| ());
            assert!(!nodes[id - 1].is_linked());
        }
        assert!(end.is_none());
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn drain_many<L: Linking, E: DrainGetEnd>(
    #[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>,
    #[values((GetFront, [1, 2, 3]), (GetBack, [3, 2, 1]))] (_end, ids): (E, [usize; 3]),
) {
    model(move || {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i + 1));
        let mut drain = pin!(list.lock().drain());
        let mut end = E::get_end(drain.as_mut());
        assert!(list.is_empty(Relaxed));
        for id in ids {
            let node = end.expect("cursor should reach every node");
            assert_eq!(node.data().0, id);
            assert!(nodes[id - 1].is_linked());
            end = node.unlink();
            assert!(!nodes[id - 1].is_linked());
        }
        assert!(end.is_none());
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

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

#[rstest]
fn cursor_move<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let _nodes: [_; 3] = array::from_fn(|i| push_node(&list, i + 1));
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        for id in [1, 2, 3] {
            assert_eq!(cursor.current().unwrap().0, id);
            cursor.move_next();
        }
        assert!(cursor.current().is_none());
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 1);
        cursor.move_prev();
        assert!(cursor.current().is_none());
        for id in [3, 2, 1] {
            cursor.move_prev();
            assert_eq!(cursor.current().unwrap().0, id);
        }
        cursor.move_prev();
        assert!(cursor.current().is_none());
    });
}

#[rstest]
fn cursor_remove_current<L: Linking>(#[values(EAGER, LAZY, SERIALIZED)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 3] = array::from_fn(|i| push_node(&list, i + 1));
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        cursor.move_next();
        assert!(cursor.remove_current());
        assert!(!nodes[1].is_linked());
        assert_eq!(cursor.current().unwrap().0, 3);
        assert!(cursor.remove_current());
        assert!(!nodes[2].is_linked());
        assert!(cursor.current().is_none());
        assert!(!cursor.remove_current());
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 1);
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
        let node = push_node(&list, 1);
        thread::scope(|scope| {
            scope.spawn(|| drop(push_node(&list, 2)));
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

#[test]
fn locked_push_back() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut nodes: [_; 3] =
            array::from_fn(|i| Box::pin(TestNode::with_data(&list, TestData(i + 1))));
        let mut locked = list.lock();
        for node in &mut nodes {
            match node.as_mut().state() {
                NodeState::Unlinked(node) => locked.push_back(node, Relaxed),
                NodeState::Linked(_) => unreachable!(),
            }
            assert!(node.is_linked());
        }
        assert_eq!(ids(&mut locked), [1, 2, 3]);
        let mut cursor = locked.cursor_front();
        while cursor.remove_current() {}
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[test]
fn cursor_insert() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut nodes: [_; 6] =
            array::from_fn(|i| Box::pin(TestNode::with_data(&list, TestData(i + 1))));
        let mut unlinked = nodes.iter_mut().map(|node| match node.as_mut().state() {
            NodeState::Unlinked(node) => node,
            NodeState::Linked(_) => unreachable!(),
        });
        let mut locked = list.lock();
        let mut cursor = locked.cursor_front();
        cursor.insert_after(unlinked.next().unwrap(), Relaxed);
        cursor.insert_after(unlinked.next().unwrap(), Relaxed);
        cursor.insert_before(unlinked.next().unwrap(), Relaxed);
        assert!(cursor.current().is_none());
        assert_eq!(ids(&mut locked), [2, 1, 3]);
        let mut cursor = locked.cursor_front();
        cursor.move_next();
        assert_eq!(cursor.current().unwrap().0, 1);
        cursor.insert_before(unlinked.next().unwrap(), Relaxed);
        cursor.insert_after(unlinked.next().unwrap(), Relaxed);
        assert_eq!(cursor.current().unwrap().0, 1);
        assert_eq!(ids(&mut locked), [2, 4, 1, 5, 3]);
        let mut cursor = locked.cursor_back();
        cursor.insert_after(unlinked.next().unwrap(), Relaxed);
        assert_eq!(ids(&mut locked), [2, 4, 1, 5, 3, 6]);
        let mut cursor = locked.cursor_back();
        assert_eq!(cursor.current().unwrap().0, 6);
        cursor.move_prev();
        assert_eq!(cursor.current().unwrap().0, 3);
        let mut cursor = locked.cursor_front();
        while cursor.remove_current() {}
        drop(locked);
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[test]
fn locked_push_back_from_node_only() {
    model(|| {
        let list = TestList::<Serialized>::new();
        let mut node = Box::pin(TestNode::with_data(&list, TestData(1)));
        let NodeState::Unlinked(unlinked) = node.as_mut().state() else {
            unreachable!()
        };
        let mut locked = aiq::list::ListRef::as_list(unlinked.list()).lock();
        locked.push_back(unlinked, Relaxed);
        drop(locked);
        assert!(node.is_linked());
    });
}
