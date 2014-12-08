from functools import wraps
import logging
import traceback

import elliptics


logger = logging.getLogger('mm.balancer')


def handler(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('Error: ' + str(e) + '\n' + traceback.format_exc())
            return {'Error': str(e)}

    return wrapper


def session_op_retry(op, acc_codes):

    def wrapped_retry(session, *args, **kwargs):
        """
        Returns tuple of (successful groups, failed_groups)
        """
        s = session.clone()

        groups = set(s.groups)
        dest_groups = set(s.groups)

        s.set_checker(elliptics.checkers.no_check)
        s.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
        s.set_filter(elliptics.filters.all_with_ack)

        retries = kwargs.pop('retries', 3)

        for i in xrange(retries):
            s.set_groups(dest_groups)
            res = getattr(s, op)(*args).get()
            success_groups = set(
                [r.group_id for r in res if r.error.code in acc_codes])
            dest_groups -= success_groups
            if not dest_groups:
                break

        return groups - dest_groups, dest_groups

    return wrapped_retry


write_retry = session_op_retry('write_data', (0,))
remove_retry = session_op_retry('remove', (0, -2))
