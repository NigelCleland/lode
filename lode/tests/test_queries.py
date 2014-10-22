from lode.database.queries import *
from nose.tools import assert_raises


def test_add_maximum_constraint():
    c1 = "Randomstring"
    v1 = "123"
    v2 = 123
    v3 = 124.57

    r1 = add_maximum_constraint(c1, v1)  # Test String
    r2 = add_maximum_constraint(c1, v2)  # Test Int
    r3 = add_maximum_constraint(c1, v3)  # Test Float\

    assert r1 == """ AND Randomstring <= '123'"""
    assert r2 == """ AND Randomstring <= '123'"""
    assert r3 == """ AND Randomstring <= '124.57'"""


def test_add_minimum_constraint():
    c1 = "Randomstring"
    v1 = "123"
    v2 = 123
    v3 = 124.57

    r1 = add_minimum_constraint(c1, v1)  # Test String
    r2 = add_minimum_constraint(c1, v2)  # Test Int
    r3 = add_minimum_constraint(c1, v3)  # Test Float\

    assert r1 == """ AND Randomstring >= '123'"""
    assert r2 == """ AND Randomstring >= '123'"""
    assert r3 == """ AND Randomstring >= '124.57'"""


def test_add_range_constraint():
    colname = "TestCol"
    b1, e1 = "37", "47"
    b2, e2 = 28, 33
    b3, e3 = 125.44, 189.76

    r1 = add_range_constraint(colname, b1, e1)  # String
    r2 = add_range_constraint(colname, b2, e2)  # Int
    r3 = add_range_constraint(colname, b3, e3)  # Float

    assert r1 == """ AND TestCol BETWEEN '37' AND '47'"""
    assert r2 == """ AND TestCol BETWEEN '28' AND '33'"""
    assert r3 == """ AND TestCol BETWEEN '125.44' AND '189.76'"""


def test_add_single_selection_constraint():
    colname = 'RandomCol'
    v1 = "Hammer"
    v2 = "47"
    v3 = 389
    v4 = 128.1231

    r1 = add_single_selection_constraint(colname, v1)  # String
    r2 = add_single_selection_constraint(colname, v2)  # String Number
    r3 = add_single_selection_constraint(colname, v3)  # Int
    r4 = add_single_selection_constraint(colname, v4)  # Float

    assert r1 == """ AND RandomCol='Hammer'"""
    assert r2 == """ AND RandomCol='47'"""
    assert r3 == """ AND RandomCol='389'"""
    assert r4 == """ AND RandomCol='128.1231'"""


def test_add_single_exclusion_constraint():
    colname = 'RandomCol'
    v1 = "Hammer"
    v2 = "47"
    v3 = 389
    v4 = 128.1231

    r1 = add_single_exclusion_constraint(colname, v1)  # String
    r2 = add_single_exclusion_constraint(colname, v2)  # String Number
    r3 = add_single_exclusion_constraint(colname, v3)  # Int
    r4 = add_single_exclusion_constraint(colname, v4)  # Float

    assert r1 == """ AND RandomCol!='Hammer'"""
    assert r2 == """ AND RandomCol!='47'"""
    assert r3 == """ AND RandomCol!='389'"""
    assert r4 == """ AND RandomCol!='128.1231'"""


def test_add_multiple_selection_constraint():

    colname = "TestCol"
    v1 = (123, 147, 158)
    v2 = [123, 146, 1231]
    v3 = ["Sample1", "Sample2", "Sample3"]
    v4 = 467

    r3 = add_multiple_selection_constraint(colname, v3)

    assert r3 == """ AND TestCol IN ('Sample1','Sample2','Sample3')"""

    assert_raises(TypeError, add_multiple_selection_constraint, colname, v1)
    assert_raises(TypeError, add_multiple_selection_constraint, colname, v2)
    assert_raises(TypeError, add_multiple_selection_constraint, colname, v4)


if __name__ == '__main__':
    pass
