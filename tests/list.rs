use std::{
    array,
    panic::{self, AssertUnwindSafe},
    pin::{Pin, pin},
    sync::atomic::Ordering::Relaxed,
};

use aiq::{
    List, Node, NodeState,
    list::{
        DrainEnd, DrainGetEnd, GetBack, GetFront, INTRUSIVE_QUEUE_MAX_STATE, Linking, ListEnd,
        ListGetEnd, LockedList,
    },
    node::{NodeData, NodeRef},
};
use linking::{EAGER, LAZY, LinkingMode};
use loom::model;
use rstest::rstest;

mod linking;
mod loom;

type TestList<L> = List<TestData, (), L>;
type TestNode<'a, L> = Node<&'a TestList<L>, TestData, (), L>;
struct TestData(usize);
impl<'a, L: Linking> NodeData<&'a TestList<L>, (), L> for TestData {
    fn new_state_if_last_node_on_drop(self: Pin<&mut Self>, _list: &&'a TestList<L>) {}
    fn on_drop<'list>(
        self: Pin<&mut Self>,
        _list: &'list &'a TestList<L>,
        _locked: Option<LockedList<'list, Self, (), L>>,
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
    List::<TestData, usize>::with_state(INTRUSIVE_QUEUE_MAX_STATE + 1);
}

#[rstest]
fn drop_non_empty_drain<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
    model(|| {
        let list = TestList::<L>::new();
        let nodes: [_; 2] = array::from_fn(|i| push_node(&list, i + 1));
        drop(list.lock().drain());
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn panic_in_drain_execute_unlocked<L: Linking>(#[values(EAGER, LAZY)] _linking: LinkingMode<L>) {
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
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
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
            end = node.unlink(|| ());
            assert!(!nodes[id - 1].is_linked());
        }
        assert!(end.is_none());
        assert!(list.is_empty(Relaxed));
        assert!(nodes.iter().all(|node| !node.is_linked()));
    });
}

#[rstest]
fn drain_many<L: Linking, E: DrainGetEnd>(
    #[values(EAGER, LAZY)] _linking: LinkingMode<L>,
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
