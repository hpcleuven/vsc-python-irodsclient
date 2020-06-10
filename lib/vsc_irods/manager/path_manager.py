import os
from vsc_irods.manager import Manager


class PathManager(Manager):
    """ A class dealing with paths on the iRODS file system """
    def __init__(self, session):
        Manager.__init__(self, session)
        self._icwd = self.get_irods_home()

    def get_irods_home(self):
        """ Returns the path to the iRODS "home" directory,
        which is e.g. used to expand '~/' in search patterns.
        """
        try:
            # In case "irods_home" was explicitly specified
            # in the irods_environment.json file:
            return self.session.pool.account.home
        except AttributeError:
            return os.path.join('/', self.session.pool.account.client_zone,
                                'home', self.session.pool.account.client_user)

    def get_irods_cwd(self):
        """ Returns the current workding directory on iRODS,
        which is e.g. used to expand '.' in search patterns.
        """
        return self._icwd

    def ichdir(self, path, verbose=False):
        """ Changes the current working directory on iRODS.

        Arguments:

        path: str
            The (absolute or relative) path to which the
            iRODS current workding directory for this session
            should be changed.

        verbose: bool (default: False)
            Set to True for more verbose output
        """
        self._icwd = self.get_absolute_irods_path(path)
        self.log('Changed iRODS CWD for this session to: %s' % self._icwd,
                 verbose)

    def get_absolute_irods_path(self, path):
        """ Returns the corresponding absolute path on the iRODS server """
        if path.startswith('/'):
            abs_path = path

        elif path.startswith('~'):
            if path.startswith('~/'):
                abs_path = os.path.join(self.get_irods_home(), path[2:])
            else:
                abs_path = os.path.join(self.get_irods_home(), path[1:])
        else:
            abs_path = os.path.join(self.get_irods_cwd(), path)

        abs_path = os.path.normpath(abs_path)
        return abs_path
