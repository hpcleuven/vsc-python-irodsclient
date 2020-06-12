import os
import glob
from irods.meta import iRODSMeta
from irods.models import Collection, DataObject
from vsc_irods.manager import Manager


def confirm(operation, kind, item):
    """ Prompts the users to confirm the given operation """
    answer = None
    while answer not in ['y', 'n']:
        answer = input('OK to %s %s %s? [y/n] ' % (operation, kind, item))
    return answer == 'y'


class BulkManager(Manager):
    """ A class for easier 'bulk' operations with the iRODS file system """

    def remove(self, iterator, recurse=False, remove_options={},
               unlink_options={}, prompt=False, verbose=False):
        """ Remove iRODS data objects and/or collections,
        in a manner that resembles the UNIX 'rm' command.

        Examples:

        >>> session.bulk.remove('tmpdir*', recurse=True)
        >>> session.bulk.remove('~/molecule_database/*.xyz')

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            search_manager.iglob() iterator). Matching data objects
            (and, if used recursively, collections) will be removed.

        TODO: remaining arguments
        """
        if isinstance(iterator, str):
            iterator = self.session.search.iglob(iterator)

        for item in iterator:
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
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)
            else:
                ok = confirm('remove', 'object', path) if prompt else True
                if ok:
                    self.log('Removing object %s' % path, verbose)
                    self.session.data_objects.unlink(path, **unlink_options)

    def get(self, iterator, local_path='.', recurse=False, get_options={},
            return_data_objects=False, verbose=False):
        """ Copy iRODS data objects and/or collections to the local machine.

        Examples:

        >>> session.bulk.get('tmpdir*', recurse=True)
        >>> session.bulk.get('~/irods_db/*.xyz', local_path='./local_db')

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            search_manager.iglob() iterator). Matching data objects
            (and, if used recursively, collections) will be copied
            to the local machine.

        TODO: remaining arguments
        """
        if isinstance(iterator, str):
            iterator = self.session.search.iglob(iterator)

        if not return_data_objects and not os.path.isdir(local_path):
            raise OSError('Destination %s does not exist' % local_path)

        objects = []

        for item in iterator:
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
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)
            else:
                if return_data_objects:
                    self.log('Getting object %s' % item, verbose)
                else:
                    self.log('Getting object %s to destination %s' % \
                             (path, local_path), verbose)

                f = None if return_data_objects else local_path
                obj = self.session.data_objects.get(path, file=f, **get_options)
                objects.append(obj)

        return objects if return_data_objects else None

    def put(self, iterator, irods_path='.', recurse=False, create_options={},
            put_options={}, verbose=False):
        """ Copy local files and/or folders to the iRODS server,
        in a manner that resembles the UNIX 'cp' command.

        Examples:

        >>> session.bulk.put('tmpdir*', recurse=True)
        >>> session.bulk.put('~/local_db/*.xyz', irods_path='./irods_db/')

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            in search_manager.iglob() iterator). Matching files on the
            local machine (and, if used recursively, directories) will
            be copied to the iRODS server.

        TODO: remaining arguments
        """
        if type(iterator) is str:
            iterator = glob.iglob(iterator)

        idest = self.session.path.get_absolute_irods_path(irods_path)

        assert self.session.collections.exists(idest), \
               'Collection %s does not exist' % idest

        for item in iterator:
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
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)

            elif os.path.isfile(item):
                self.log('Putting object %s in collection %s' % \
                         (item, idest), verbose)
                self.session.data_objects.put(item, idest + '/',
                                              **put_options)

    def add_metadata(self, iterator, recurse=False, collection_avu=[],
                     object_avu=[], verbose=False):
        """ Add metadata to iRODS data objects and/or collections.

        Examples:

        >>> session.bulk.add_metadata('tmpdir*', recurse=True,
                                      object_avu=('is_temporary_file',)),
                                      collection_avu=('is_temporary_dir',))

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            search_manager.iglob() iterator). Metadata will be added to
            matching data objects and, if used recursively, collections.

        TODO: remaining arguments
        """
        if isinstance(iterator, str):
            iterator = self.session.search.iglob(iterator)

        if isinstance(object_avu, tuple): object_avu = [object_avu]
        if isinstance(collection_avu, tuple): collection_avu = [collection_avu]

        for item in iterator:
            path = self.session.path.get_absolute_irods_path(item)

            if self.session.collections.exists(path):
                # Item is a collection, not an object
                if recurse:
                    for avu in collection_avu:
                        self.log('Adding metadata to collection %s' % path,
                                 verbose)
                        meta = iRODSMeta(*avu)
                        self.session.metadata.set(Collection, path, meta)

                    self.add_metadata(item + '/*', recurse=True,
                                      collection_avu=collection_avu,
                                      object_avu=object_avu,
                                      verbose=verbose)
                else:
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)
            else:
                for avu in object_avu:
                    self.log('Adding metadata to object %s' % path, verbose)
                    meta = iRODSMeta(*avu)
                    self.session.metadata.set(DataObject, path, meta)
