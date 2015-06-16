from mastermind.service import ReconnectableService


class MastermindClient(object):
    """Provides python binding to mastermind cocaine application.
    """
    def __init__(self):
        pass

    def group_info(self, group_id):
        """
        Get detailed group information.

        Args:
          group_id: group integer identificator, e.g. 42.

        Returns:
          A dict representing detailed group information, i.e. its status,
          couple and namespace (if applicable), detailed node backend information.
        """
        raise NotImplemented

    def couple_info(self, couple_id):
        """
        Get detailed couple information.

        Args:
          couple_id: couple string identificator (groups identificators joined via colon).

        Returns:
          A dict representing detailed couple information, i.e. its status,
          minimal couple statistics, detailed couple group information (equivalent to
          <group_info> request for each coupled group).
        """
        raise NotImplemented

    COUPLE_INIT_COUPLED_STATE = 'coupled'
    COUPLE_INIT_FROZEN_STATE = 'frozen'

    def couple_build(self, couple_size, namespace, state,
                     couples=1, groups=None, ignore_space=False, dry_run=False):
        """
        Builds a number of couples to extend a namespace.

        Args:
          couple_size: a number of groups to couple together.
          namespace: all created couples will belong to provided namespace.
          state: couple init state (should take one of COUPLE_INIT_*_STATE values).
          couples: number of couples that mastermind will try to create.
          groups: iterable of sets of mandatory groups that should be coupled together:
            Example: ((42, 69), # groups 42 and 69 will be included in the first created couple,
                      (128))    # group 128 will be included in the second one, and so on.
          ignore_space: if this flag is set to True mastermind will couple only the groups
            having equal total space
          dry_run: build couple in dry-run mode.
            Mastermind will not write corresponding metakeys to selected groups,
            which effectively means that couples will not be created.

        Returns:
          List of created couples' ids.
        """
        raise NotImplemented

    UNCOUPLED_GROUP_BAD_STATE = 'bad'
    UNCOUPLED_GROUP_GOOD_STATE = 'good'

    def group_list_uncoupled(self, state=UNCOUPLED_GROUP_GOOD_STATE, in_service=False):
        """
        Get list of uncoupled groups that are initialized and not coupled with any other
        group.

        Args:
          state: filter groups by its state (should take one of UNCOUPLED_GROUP_*_STATE).
          in_service: should response include uncoupled group ids that are in service
            at the moment (e.g. participating in jobs, etc.).

        Returns:
          List of uncoupled groups' ids.
        """
        raise NotImplemented

    def ns_settings(self, namespace, service_key=False, deleted=False):
        """
        Get namespace settings.

        Args:
          namespace: kinda obvious...
          service_key: should the response include internal __service key that is used
            for storing namespace service data.
          deleted: if the flag is set mastermind will return settings even if namespace
            has already been deleted.

        Returns:
          A dict represening various namespace settings. A common set of settings' options
          can be overviewed in the description of <ns_setup> method.
        """
        raise NotImplemented

    def ns_setup(self, namespace,
                 static_couple=None,
                 overwrite=None, groups_count=None, success_copies=None,
                 auth_key_write=None, auth_key_read=None,
                 sign_token=None, sign_path_prefix=None,
                 min_units=None, add_unit=None,
                 redirect_content_length_threshold=None, redirect_expire_time=None,
                 multipart_content_length_threshold=None,
                 select_couple_to_upload=None,
                 reserved_space_percentage=None,
                 check_for_update=None,
                 custom_expiration_time=None):
        """
        Performs namespace setup.

        Can be used for inital namespace setup as well as specific option update.

        Args:
          namespace: kinda obvious...
          overwrite: set namespace settings from scratch using only currently
            provided options
          static_couple: static couple's string identificator. Used when namespace
            does not store metainformation and therefore group balancing is not
            applicable.
          groups_count: default number of groups per couple.
          success_copies: client's success copy politics !!!!! (any|quorum|all). !!!!!
          auth_key_write: proxy auth-key for writing to namespace.
          auth_key_read: proxy auth-key for reading from namespace.
          sign_token: signature token
          sign_path_prefix: signature path prefix
          min_units: minimal number of couples available for write operations
            when namespace is considered available
          add_unit: number of additional couples with positive weights
            that mastermind will add to namespace group weights if available
          redirect_content_length_threshold: content length threshold for
            proxy to return direct urls instead of balancer urls
          redirect_expire_time: period of time for which redirect url
                is considered valid
          multipart_content_length_threshold: this flag enables multipart upload for
            requests with content length less than threshold if this flag is True
          select_couple_to_upload: this flag allows client to manually select a couple
            to write key to
          reserved_space_percentage: percentage of effective space that
            will be reserved for future updates, couple will be closed when
            free effective space percentage is less than or equal to reserved
            space percentage
          check_for_update: this flag allows to upload the key to namespace only
            if does not exists already
          custom_expiration_time: allows namespace to use expire-time argument
            for signing url

          Returns:
            True
        """
        raise NotImplemented
