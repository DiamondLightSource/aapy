from aa import pb
import pytest


def test_binary_search_raises_IndexError_for_empty_seq():
    with pytest.raises(IndexError):
        pb.binary_search([], lambda x: x, 1) == -1


def test_binary_search_returns_zero_if_target_below_range():
    f = lambda x: x
    assert pb.binary_search([2, 3, 4], f, 1) == 0


def test_binary_search_returns_len_seq_if_target_above_range():
    f = lambda x: x
    assert pb.binary_search([1, 2, 3], f, 4) == 3


def test_binary_search_returns_lower_index():
    f = lambda x: x
    assert pb.binary_search([1, 2], f, 1.5) == 0


def test_binary_search_returns_index_if_value_equals_item_in_seq():
    f = lambda x: x
    assert pb.binary_search([1, 2], f, 1) == 0
