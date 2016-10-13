from logging import getLogger
from yql.api.v1.client import YqlClient
import time
import datetime


logger = getLogger('mm.planner')


class YqlWrapper(object):

    VALIDATE_QUERY = """
        SELECT
          COUNT(*)
        FROM [{table}]
        WHERE source_table="{date_iso}";
    """

    # XXX: maybe something more optimal could be done? How the db is organized?
    # XXX: tounixdate -> arithmetic ops
    # XXX: use HAVING ?
    PREAGGREGATE_QUERY = """
$dateconv = @@
import time
import datetime
def tounixdate(exp_time):
    return int(time.mktime((datetime.date.fromtimestamp(exp_time) + datetime.timedelta(days=1)).timetuple()))
@@;

$dateconvert = Python::tounixdate("(Uint64?)->Uint64", $dateconv);

INSERT INTO [{agr_table}]
SELECT
    couple_id,
    expiration_date,
    namespace,
    "upload" AS operation,
    SUM(CAST(object_size AS Int64)) AS expired_size,
    "{date_iso}" AS source_table,
    {timestamp} AS timestamp
FROM [{main_table}]
WHERE op="upload"
GROUP BY
    couple_id,
    namespace,
    $dateconvert(CAST(expire_at AS Uint64)) AS expiration_date;

INSERT INTO [{agr_table}]
SELECT
    couple_id,
    expiration_date,
    namespace,
    "delete" AS operation,
    -1*SUM(CAST(object_size AS Int64)) AS expired_size,
    "{date_iso}" AS source_table,
    {timestamp} AS timestamp
FROM [{main_table}]
WHERE op="delete" AND expire_at IS NOT NULL
GROUP BY
    couple_id,
    namespace,
    $dateconvert(CAST(expire_at AS Uint64)) AS expiration_date;
        """

    AGGREGATE_QUERY = """
        SELECT couple_id
        FROM
            (SELECT
                couple_id,
                SUM(expired_size) AS sum_expired_size
            FROM [{table}]
            WHERE expiration_date <= {timestamp}
            GROUP BY couple_id)
        WHERE  sum_expired_size >= {trigger};
    """

    def __init__(self, cluster, token, attempts, delay):
        self._cluster = cluster
        self._token = token
        self._attempts = attempts
        self._delay = delay

    def send_request(self, query, timeout=None):
        """
        Send request to YQL: ping verification + attempts retry
        @return result dict on success, excepts otherwise
        """
        logger.debug('Send YQL request {}'.format(query))
        result = None

        with YqlClient(db=self._cluster, token=self._token) as yql:
            if not yql.ping():
                raise IOError("YQL ping failed {} {}".format(self._cluster, self._token[:20]))

            for attempt in xrange(self._attempts):
                try:
                    start_time = time.time()

                    request = yql.query(query)
                    request = request.run()
                    logger.info("YQL query is running with id {}".format(request.operation_id))
                    result = request.results
                    end_time = time.time()
                    if result.is_success:
                        logger.info("YQL query succeeded and took {}".format(end_time-start_time))
                        if timeout and (end_time - start_time > timeout):
                            # XXX: need to add this condition into monitoring
                            # Do not except here - after all we already have some successful result
                            logger.error("YQL query({}..{}) > timeout {}".format(start_time, end_time, timeout))
                        return result

                    logger.error("YQL failed #{}: {}[{}] ({},{})".format(attempt, result.status,
                                 request.status_code, request.explain(), str(request.exc_info)))
                    for error in result.errors:
                        logger.error("YQL query result error {}".format(str(error)))

                    time.sleep(self._delay)
                except:
                    logger.exception("YQL request excepted")
                    continue  # give one more chance

        raise RuntimeError("YQL request attempts has exhausted {} {}".format(self._attempts, query))


    def request_expired_stat(self, aggregate_table, expired_threshold, timeout=None):
        """
        Send request to aggregate table to find volume of expired space in couples
        :param aggregate_table: the name of aggregation table
        :param expired_threshold: couples with expired volume above the param would be returned
        :return list of couple ids with expired data more than trigger
        """

        timestamp = int(time.time())
        query = self.AGGREGATE_QUERY.format(table=aggregate_table, timestamp=timestamp, trigger=expired_threshold)

        try:
            r = self.send_request(query, timeout)
            if r.rows_count == 0:
                # it is quite suspicious that we haven't found anything. Maybe wrong table?
                logger.warning("empty result for table {} and trigger {}".format(aggregate_table, expired_threshold))
                return []
        except:
            logger.exception("YT request excepted")
            return []

        valid_couples = []

        for table in r.results:  # access to results blocks until they are ready
            table.fetch_full_data()
            for row in table.rows:
                try:
                    couple = int(row[0])
                except ValueError:
                    logger.exception("couple {} is invalid".format(str(row[0])))
                else:
                    logger.debug("couple {} is valid".format(couple))
                    valid_couples.append(couple)

        return valid_couples

    def prepare_aggregate_for_yesterday(self, base_table, aggregate_table):
        """
        Update aggregate table from YT with data from yesterday logs if needed
        :param base_table: YT table with log records filled by mds-proxy
        :param aggregate_table: the name of aggregate table
        """
        yesterday = datetime.date.today() - datetime.timedelta(days=1)

        # check for need to run an aggregation query
        query = self.VALIDATE_QUERY.format(table=aggregate_table, date_iso=yesterday.isoformat())
        res = self.send_request(query)
        try:
            tbl = next(x for x in res.results)
            row = next(x for x in tbl.rows)
            count = int(row[0])
        except:
            logger.exception("Analysis of validation results has excepted")
            raise

        if not count:
            self.prepare_aggregate_table(base_table, aggregate_table, yesterday)
        else:
            logger.info("Skip write into aggregation table due to {} records from {}".format(count, str(yesterday)))

    def prepare_aggregate_table(self, base_table, aggregate_table, date):
        """
        Update aggregate table from base_table/date
        :param base_table: YT table with log records filled by mds-proxy
        :param aggregate_table: the name of aggregate_table (where to store data)
        :param date: date for which we extract log records that would be added to aggregate_tbale
        """
        date_iso = date.isoformat()
        main_table_per_day = "{}/{}".format(base_table, date_iso)

        query = self.PREAGGREGATE_QUERY.format(agr_table=aggregate_table, main_table=main_table_per_day,
                                               timestamp=int(time.time()), date_iso=date_iso)

        return self.send_request(query)
