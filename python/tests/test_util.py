import pytest

from deltalake._util import deprecate_positional_commit_args


def test_no_args_returns_kwargs_unchanged():
    commit_props = object()
    post_hook_props = object()
    result = deprecate_positional_commit_args("foo", (), commit_props, post_hook_props)
    assert result == (commit_props, post_hook_props)


def test_positional_arg_emits_deprecation_warning():
    commit_props = object()
    with pytest.warns(DeprecationWarning, match="foo\\(\\)"):
        result = deprecate_positional_commit_args("foo", (commit_props,), None, None)
    assert result == (commit_props, None)


def test_positional_args_canonical_order():
    commit_props = object()
    post_hook_props = object()
    with pytest.warns(DeprecationWarning):
        result = deprecate_positional_commit_args("foo", (commit_props, post_hook_props), None, None)
    assert result == (commit_props, post_hook_props)


def test_positional_args_reversed_legacy_order():
    commit_props = object()
    post_hook_props = object()
    with pytest.warns(DeprecationWarning):
        result = deprecate_positional_commit_args(
            "foo",
            (post_hook_props, commit_props),
            None,
            None,
            legacy_order=("post_commithook_properties", "commit_properties"),
        )
    assert result == (commit_props, post_hook_props)


def test_extra_positional_args_raises_type_error():
    with pytest.raises(TypeError, match="takes at most 2"):
        deprecate_positional_commit_args("foo", (object(), object(), object()), None, None)


def test_keyword_and_positional_conflict_raises_type_error():
    commit_props = object()
    with pytest.raises(TypeError, match="multiple values for 'commit_properties'"):
        deprecate_positional_commit_args("foo", (commit_props,), commit_properties=commit_props)
