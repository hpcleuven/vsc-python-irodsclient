import os
import fnmatch
import warnings
from vsc_irods.manager import Manager


class SearchManager(Manager):
    """ A class for easier searching in the iRODS file system """
    def glob(self, pattern, debug=False):
        """ Returns a list of iRODS collection and data object paths
        which match the given pattern, similar to the glob.glob builtin.

        .. note::
 
            Currently only '*' is expanded. The other special characters
            '?' and '[]' are not (yet) taken into account.

        Examples:

        >>> session.glob('m*/ch4.xyz')
            ['molecules_database/ch4.xyz']
        >>> session.glob('./*/*')
            ['./molecule_database/a.out', './foo/bar.so']
        >>> session.glob('~/foo/c*.xyz')
            ['~/foo/ch4.xyz', '~/foo/co2.xyz']

        Arguments:

        pattern: str
            The search pattern

        debug: bool (default: False)
            Set to True for debugging info
        """
        self.log('DBG| glob pattern = %s' % pattern, debug)

        results = []

        if '*' not in pattern:
            path = self.session.path.get_absolute_irods_path(pattern)

            if (self.session.data_objects.exists(path) or
                self.session.collections.exists(path)):
                results.append(pattern)
        else:
            index = pattern.index('*')
            path = os.path.dirname(pattern[:index])

            remainder = pattern[len(path)+1:] if len(path) > 0 else pattern

            self.log('DBG| path = %s' % path, debug)
            self.log('DBG| remainder = %s' % remainder, debug)

            gen = self.find(irods_path=path, pattern=remainder, types='d,f',
                            use_wholename=True, topdown=True, debug=debug)

            results.extend([hit for hit in gen])

        self.log('DBG| returning %s' % results, debug)
        return results

    def find(self, irods_path='.', pattern='*', use_wholename=False,
             types='d,f', topdown=True, debug=False):
        """ Returns a list of iRODS collection and data object paths
        which match the given pattern, similar to the UNIX `find` command.

        Examples:

        >>> session.find('.', name='*mol*/c*.xyz', types='f')
            ['data/molecules/c6h6.xyz', './data/molecules/ch3cooh.xyz']
        >>> session.find('~/data', name='molecules', types='d')
            ['~/data/molecules']

        Arguments:

        irods_path: str (default: '.')
            Root of the directory tree in which to search

        pattern: str (default: '*')
            The search pattern

        use_wholename: bool (default: False)
            Whether it is the whole (absolute) path name that has to
            match the pattern, or only the basename of the collection
            or data object.

        types: str (default: 'd,f')
            Comma-separated list of one or more of the following characters
            to select the type of results to include:

            * 'd' for directories (i.e. collections)
            * 'f' for files (i.e. data objects)

        topdown: bool (default: True)
            Whether to start from the top directory and go down the tree,
            or to start from the bottom and climb up the tree. This only
            affects the order of the results.

		debug: bool (default: False)
            Set to True for debugging info
        """
        def get_wholename(abs_path, abs_root):
            wholename = os.path.join(irods_path, abs_path[len(abs_root)+1:])
            self.log('DBG| abs_path = %s ; abs_root = %s ; wholename = %s' % \
                     (abs_path, abs_root, wholename), debug)
            return wholename

        path = self.session.path.get_absolute_irods_path(irods_path)
        root = self.session.collections.get(path)

        if not use_wholename and '/' in pattern:
            msg = "Pattern %s contains a slash. UNIX file names usually don't, "
            msg += "so this search will probably yield no results. Setting "
            msg += "'wholename=True' may help you find what you're looking for."
            warning.warn(msg % pattern)

        self.log('DBG| find pattern = %s' % pattern, debug)

        for (collection, subcollections, objects) in root.walk(topdown=topdown):
            self.log('DBG| collection = %s' % collection.name, debug)

            for t in types.split(','):
                if t == 'f':
                    for obj in objects:
                        wholename = get_wholename(obj.path, path)
                        name = wholename if use_wholename else obj.name
                        if fnmatch.fnmatch(name, pattern):
                            yield wholename

                elif t == 'd':
                    for subcoll in subcollections:
                        wholename = get_wholename(subcoll.path, path)
                        name = wholename if use_wholename else subcoll.name
                        if fnmatch.fnmatch(name, pattern):
                            yield wholename
