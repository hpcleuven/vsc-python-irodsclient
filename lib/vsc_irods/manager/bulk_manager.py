import os
import glob
from vsc_irods.manager import Manager


def confirm(operation, kind, item):
    """ Prompts the users to confirm the given operation """
    answer = None
    while answer not in ['y', 'n']:
        answer = input('OK to %s %s %s? [y/n] ' % (operation, kind, item))
    return answer == 'y'


class BulkManager(Manager):
    """ A class for easier 'bulk' operations with the iRODS file system """

    def remove(self, *patterns, recurse=False, remove_options={},
               unlink_options={}, prompt=False, verbose=False):
        """ Remove iRODS data objects and/or collections,
        in a manner that resembles the UNIX 'rm' command.

        Examples:

        >>> session.bulk.remove('tmpdir*', recurse=True)
        >>> session.bulk.remove('~/molecule_database/*.xyz')

        Arguments:

        patterns: (list of) str
            One or more search patterns. Matching data objects
            (and, if used recursively, collections) will be removed.

        TODO: remaining arguments
        """
        for pattern in patterns:
            hits = self.session.search.glob(pattern)

            if len(hits) == 0:
                # Always print this as a warning
                self.log('Cannot remove %s (no such file or directory)' % \
                         pattern)

            for item in hits:
                path = self.session.path.get_absolute_irods_path(item)

                if self.session.collections.exists(path):
                    # Item is a collection, not an object
                    if recurse:
                        ok = confirm('remove', 'collection', path) if prompt \
                             else True
                        if ok:
                            self.log('Removing collection %s' % path, verbose)
                            self.session.collections.remove(path, recurse=True,
                                                            **remove_options)
                    else:
                    	self.log('Skipping collection %s (no recursion)' % \
                    	         item, verbose)
                else:
                    ok = confirm('remove', 'object', path) if prompt else True
                    if ok:
                        self.log('Removing object %s' % path, verbose)
                        self.session.data_objects.unlink(path, **unlink_options)

    def get(self, *patterns, local_path='.', recurse=False, get_options={},
            return_data_objects=False, verbose=False):
        """ Copy iRODS data objects and/or collections to the
		local machine, in a manner that resembles the UNIX 'cp' command.

        Examples:

        >>> session.bulk.get('tmpdir*', recurse=True)
        >>> session.bulk.get('~/irods_db/*.xyz', local_path='./local_db')

        Arguments:

        patterns: (list of) str
            One or more search patterns. Matching data objects
            (and, if used recursively, collections) will be copied
            to the local machine.

        TODO: remaining arguments
        """
        more_than_one_item = sum([len(self.session.search.glob(p))
                                  for p in patterns]) > 1

        if more_than_one_item and not return_data_objects:
            assert os.path.isdir(local_path), \
                   'Destination %s does not exist' % local_path

        objects = []
        for pattern in patterns:
            hits = self.session.search.glob(pattern)

            if len(hits) == 0:
                # Always print this as a warning
                self.log('Cannot get %s (no such file or directory)' % pattern,
                         True)

            for item in hits:
                path = self.session.path.get_absolute_irods_path(item)

                if self.session.collections.exists(path):
                    # Item is a collection, not an object
                    if recurse:
                        d = os.path.join(local_path, os.path.basename(item))

                        if not os.path.exists(d) and not return_data_objects:
                            self.log('Creating directory: %s' % d, verbose)
                            os.mkdir(d)

                        self.get(item + '/*', local_path=d, recurse=True,
                                 get_options=get_options,
                                 return_data_objects=return_data_objects,
                                 verbose=verbose)
                    else:
                        self.log('Skipping collection %s (no recursion)' % \
                                 item, verbose)
                else:
                    if return_data_objects:
                        self.log('Getting object %s' % item, verbose)
                    else:
                        self.log('Getting object %s to destination %s' % \
                                 (path, local_path), verbose)

                    f = None if return_data_objects else local_path
                    obj = self.session.data_objects.get(path, file=f, 
                                                        **get_options)
                    objects.append(obj)

        return objects if return_data_objects else None

    def put(self, *patterns, irods_path='.', recurse=False, create_options={},
            put_options={}, verbose=False):
        """ Copy local files and/or folders to the iRODS server,
        in a manner that resembles the UNIX 'cp' command.

        Examples:

        >>> session.bulk.put('tmpdir*', recurse=True)
        >>> session.bulk.get('~/local_db/*.xyz', irods_path='./irods_db/')

        Arguments:

        patterns: (list of) str
            One or more search patterns. Matching files on the local machine
            (and, if used recursively, collections) will be copied to the
            iRODS server.

        TODO: remaining arguments
        """
        more_than_one_item = sum([len(glob.glob(p)) for p in patterns]) > 1

        idest = self.session.path.get_absolute_irods_path(irods_path)

        if more_than_one_item:
            assert self.session.collections.exists(idest), \
                   'Collection %s does not exist' % idest

        for pattern in patterns:
            hits = glob.glob(pattern)

            if len(hits) == 0:
				# Always print this as a warning
                self.log('Cannot put %s (no such file or directory)' % pattern,
                         True)

            for item in hits:
                if os.path.isdir(item):
                    if recurse:
                        d = os.path.basename(item)
                        d = os.path.join(idest, d)

                        if not self.session.collections.exists(d):
                            self.log('Creating collection: %s' % d, verbose)
                            self.session.collections.create(d, recurse=True,
                                                            **create_options)

                        self.put(item + '/*', irods_path=d, recurse=True,
                                 create_options=create_options,
                                 put_options=put_options, verbose=verbose)
                    else:
                        self.log('Skipping collection %s (no recursion)' % \
                                 item, verbose)

                elif os.path.isfile(item):
                    if not more_than_one_item:
                        # TODO: check for existence and prompt for overwrite?
                        pass
                    self.log('Putting object %s in collection %s' % \
                             (item, idest), verbose)
                    self.session.data_objects.put(item, idest + '/',
                                                  **put_options)

    def add_metadata(self, *patterns, avu=[]):
        """ To be implemented """
        pass
