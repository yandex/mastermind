import random
import string

import pytest


@pytest.fixture(scope='function')
def ascii_data(data_size):
    return ''.join(
        random.choice(
            string.ascii_letters + string.digits
        ) for _ in xrange(data_size)
    )


def parametrize(argnames, argvalues, arglabels={}, **kwds):
    """Overrides 'pytest.mark.parametrize' implementation with
    automatic argvalue labels generation.

    Arguments:
        argnames - a comma-separated string denoting one or more argument names,
            or a list/tuple of argument strings.
        argvalues - the list of argvalues determines how often a test is invoked with
            different argument values. If only one argname was specified argvalues
            is a list of values. If N argnames were specified, argvalues must be
            a list of N-tuples, where each tuple-element specifies a value
            for its respective argname.
        arglabels - maps argname to a label that will be printed out along with
            corresponding argvalue on a test run. This argument helps to construct
            @ids parameter for underlying @pytest.mark.parametrize call. If argname
            is not present in arglabels it will be used instead of label.
            If @ids is supplied it will be passed as is to @pytest.mark.parametrize.
        **kwds - any other arguments for @pytest.mark.parametrize.
    """

    if 'ids' in kwds:
        ids = kwds.pop('ids')
    else:
        args = [a.strip() for a in argnames.split(',')]
        ids_tpls = ['{arg} {{value}}'.format(arg=arglabels.get(a, a)) for a in args]
        ids = []
        for argvalue in argvalues:
            if not isinstance(argvalue, (list, tuple, basestring)):
                argvalue = [argvalue]
            ids.append(
                ', '.join(
                    ids_tpl.format(value=argvalue[i])
                    for i, ids_tpl in enumerate(ids_tpls)
                )
            )

    def wrapper(func):
        return pytest.mark.parametrize(
            argnames,
            argvalues,
            ids=ids,
            **kwds
        )(func)

    return wrapper
