import os
import fnmatch
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
        self.log('DBG| processing pattern %s' % pattern, debug)

        if '*' not in pattern:
            path = self.session.path.get_absolute_irods_path(pattern)
            results = []
            if (self.session.data_objects.exists(path) or
                self.session.collections.exists(path)):
                results.append(pattern)

            self.log('DBG| returning %s' % results, debug)
            return results

        index = pattern.index('*')
        basedir = os.path.dirname(pattern[:index])
        remainder = pattern[len(basedir)+1:] if len(basedir) > 0 else pattern
        basedir_abs = self.session.path.get_absolute_irods_path(basedir)
        collection = self.session.collections.get(basedir_abs)

        self.log('DBG| basedir = %s' % basedir, debug)
        self.log('DBG| basedir_abs = %s' % basedir_abs, debug)
        self.log('DBG| remainder = %s' % remainder, debug)

        results = []
        if '/' not in remainder:
            for coll in collection.subcollections:
                if fnmatch.fnmatch(coll.name, remainder):
                    results.append(os.path.join(basedir, coll.name))

            for obj in collection.data_objects:
                if fnmatch.fnmatch(obj.name, remainder):
                    results.append(os.path.join(basedir, obj.name))
        else:
            index = remainder.index('/')
            common = os.path.join(basedir_abs, remainder[:index])
            self.log('DBG| common = %s' % common, debug)

            for coll in collection.subcollections:
                d = os.path.join(basedir_abs, coll.name)
                match = fnmatch.fnmatch(d, common)
                self.log('DBG| coll.name = %s ; d = %s ; match = %s' % \
                         (coll.name, match), d, debug)
                if match:
                    p = os.path.join(basedir, coll.name,   remainder[index+1:])
                    self.log('DBG| recursion with pattern p = %s' % p, debug)
                    results.extend(self.glob(p))

        self.log('DBG| returning %s' % str(results), debug)
        return results

    def find(self, pattern):
        """ To be implemented """
        pass
