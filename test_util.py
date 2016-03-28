#!/usr/bin/env python
"""Test utils."""


from util import table_name_from, column_prefix_from, columns_from, tables_from


def test_table_name_from():
    """Test table name from query."""
    assert table_name_from('users', 'posts.3.comments.5') == 'users_posts_comments'
    assert table_name_from('users', '') == 'users'
    assert table_name_from('users', 'posts.3.comments') == 'users_posts'
    assert table_name_from('users', 'posts.0') == 'users_posts'
    assert table_name_from('users', '') == 'users'
    assert table_name_from('users', 'posts.3.comments.likes') == 'users_posts'
    assert table_name_from('users', 'comments.likes') == 'users'


def test_tables_from():
    """Test tables from."""
    assert list(tables_from('users', 'posts.3.comments.5')) == [
        'users', 'users_posts', 'users_posts_comments']
    assert list(tables_from('users', '')) == ['users']
    assert list(tables_from('users', 'posts.3.comments')) == [
        'users', 'users_posts']
    assert list(tables_from('users', 'posts.0')) == [
        'users', 'users_posts']
    assert list(tables_from('users', '')) == ['users']
    assert list(tables_from('users', 'posts.3.comments.likes')) == [
        'users', 'users_posts']


def test_column_name_from():
    """Test column name from query."""
    assert column_prefix_from('users', 'posts.3.comments.5') == ''
    assert column_prefix_from('users', '') == ''
    assert column_prefix_from('users', 'posts.3.comments') == 'comments'
    assert column_prefix_from('users', 'posts.0') == ''
    assert column_prefix_from('users', '') == ''
    assert column_prefix_from('users', 'posts.3.comments.likes') == 'comments_likes'
    assert column_prefix_from('users', 'comments.likes') == 'comments_likes'


def test_column_names():
    """Test column names for object."""
    assert list(columns_from('', {'a': '', 'b': ''})) == ['a', 'b']
    assert list(columns_from('users', {'a': '', 'b': ''})) == ['users_a', 'users_b']
    assert list(columns_from('a_users', {'a': '', 'b': ''})) == ['a_users_a', 'a_users_b']
