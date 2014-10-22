def add_equality_constraint(column, values):

    if not hasattr(values, '__iter__'):
        return add_single_selection_constraint(column, values)
    else:
        return add_multiple_selection_constraint(column, values)


def add_exclusion_constraint(column, values):

    if not hasattr(values, '__iter__'):
        return add_single_exclusion_constraint(column, values)
    else:
        return add_multiple_exclusion_constraint(column, values)


def add_minimum_constraint(column, value):
    return """ AND %s >= '%s'""" % (column, value)


def add_maximum_constraint(column, value):
    return """ AND %s <= '%s'""" % (column, value)


def add_range_constraint(column, begin, end):
    return """ AND %s BETWEEN '%s' AND '%s'""" % (column, begin, end)


def add_single_selection_constraint(column, value):
    return """ AND %s='%s'""" % (column, value)


def add_multiple_selection_constraint(column, values):
    joined = "','".join(values)
    jvalues = "('%s')" % joined
    return """ AND %s IN %s""" % (column, jvalues)


def add_single_exclusion_constraint(column, value):
    return """ AND %s!='%s'""" % (column, value)


def add_multiple_exclusion_constraint(column, values):
    joined = "','".join(values)
    jvalues = "('%s')" % joined
    return """ AND %s NOT IN %s""" % (column, jvalues)

if __name__ == '__main__':
    pass
