#[test]
fn structural_fixture_is_not_an_empty_stub() {
    assert!(!std::mem::size_of::<super::view::ForkTreeReadFacade<'_>>().eq(&0));
}
