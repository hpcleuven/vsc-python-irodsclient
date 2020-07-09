import os
import glob
from irods.keywords import FORCE_FLAG_KW
from irods.meta import iRODSMeta
from irods.models import Collection, DataObject
from vsc_irods.manager import Manager


# Job-related environment variables used by add_job_metadata
job_env_var = ['PBS_O_HOST', 'PBS_JOBID', 'PBS_JOBNAME', 'PBS_NODEFILE',
               'SLURM_SUBMIT_HOST', 'SLURM_JOB_ID', 'SLURM_JOB_NAME',
               'SLURM_JOB_NODELIST']


def confirm(operation, kind, item):
    """ Prompts the users to confirm the given operation """
    answer = None
    while answer not in ['y', 'n']:
        answer = input('OK to %s %s %s? [y/n] ' % (operation, kind, item))
    return answer == 'y'


class BulkManager(Manager):
    """ A class for easier 'bulk' operations with the iRODS file system """

    def remove(self, iterator, recurse=False, force=False, prompt=False,
               verbose=False, **options):
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

        recurse: bool (default: False)
            Whether to use recursion, meaning that also matching collections
            and their data objects and subcollections will be removed.

        force: bool (default: False)
            Whether to imediately remove the collections or data objects,
            without putting them in the trash.

        prompt: bool (default: False)
            Whether to prompt for permission before removing a data
            object or collection.

        verbose: bool (default: False)
            Whether to print more output.

        options: (any remaining keywords arguments)
            Additional options to be passed on to PRC's
            collections.remove() and data_objects.unlink() methods.
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
                                                        force=force, **options)
                else:
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)
            else:
                ok = confirm('remove', 'object', path) if prompt else True
                if ok:
                    self.log('Removing object %s' % path, verbose)
                    self.session.data_objects.unlink(path, force=force,
                                                     **options)

    def get(self, iterator, local_path='.', recurse=False, force=False,
            return_data_objects=False, verbose=False, **options):
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

        local_path: str (default: '.')
            The (absolute or relative) path on the local file system
            where the data objects and collections wil be copied to.

        recurse: bool (default: False)
            Whether to use recursion, meaning that also matching collections
            and their data objects and subcollections will be copied.

        force: bool (default: False)
            Whether to overwrite existing local files.

        return_data_objects: (default: False)
            Whether to return a list of iRODSDataObject instances
            of the uploaded data objects.

        verbose: bool (default: False)
            Whether to print more output.

        options: (any remaining keywords arguments)
            Additional options to be passed on to PRC's
            data_objects.get() method.
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
                             return_data_objects=return_data_objects,
                             verbose=verbose, force=force, **options)
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
                extra_options = {FORCE_FLAG_KW: ''} if force else {}
                obj = self.session.data_objects.get(path, file=f, **options,
                                                    **extra_options)
                objects.append(obj)

        return objects if return_data_objects else None

    def put(self, iterator, irods_path='.', recurse=False, force=False,
            verbose=False, create_options={}, **options):
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

        irods_path: str (default: '.')
            The (absolute or relative) path on the iRODS file system
            where the local files and folders wil be copied to.

        recurse: bool (default: False)
            Whether to use recursion, meaning that also matching folders
            and their files and subfolders will be copied to the iRODS server.

        force: bool (default: False)
            Whether to overwrite existing data objects.

        verbose: bool (default: False)
            Whether to print more output.

        create_options: dict (default: {})
            Additional options to be passed on to PRC's
            collections.create() method.

        options: (any remaining keywords arguments)
            Additional options to be passed on to PRC's
            data_objects.put() method.
        """
        if type(iterator) is str:
            iterator = glob.iglob(iterator)

        idest = self.session.path.get_absolute_irods_path(irods_path)

        assert self.session.collections.exists(idest), \
               'Collection %s does not exist' % idest

        for item in iterator:
            item = item.rstrip('/')

            if os.path.isdir(item):
                if recurse:
                    d = os.path.basename(item)
                    d = os.path.join(idest, d)

                    if not self.session.collections.exists(d):
                        self.log('Creating collection: %s' % d, verbose)
                        self.session.collections.create(d, recurse=True,
                                                        **create_options)

                    self.put(item + '/*', irods_path=d, recurse=True,
                             force=force, create_options=create_options,
                             verbose=verbose, **options)
                else:
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)

            elif os.path.isfile(item):
                self.log('Putting object %s in collection %s' % \
                         (item, idest), verbose)
                self.session.data_objects.put(item, idest + '/', force=force,
                                              **options)

    def metadata(self, iterator, action='add', recurse=False, collection_avu=[],
                 object_avu=[], verbose=False):
        """ Add or remove metadata to iRODS data objects and/or collections.

        Examples:

        >>> session.bulk.metadata('tmpdir*', action='add', recurse=True,
                                   object_avu=('is_temporary_file',)),
                                   collection_avu=('is_temporary_dir',))

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            search_manager.iglob() iterator). Metadata will be modified for
            matching data objects and, if used recursively, collections.

        action: str
            The action to perform. Choose either 'add' or 'remove'.

        recurse: bool (default: False)
            Whether to use recursion, meaning that metadata will be
            modified for matching collections and their data objects
            and subcollections.

        collection_avu: tuple or list of tuples (default: [])
            One or several attribute-value[-unit]] tuples to be modified
            for collections.

        object_avu: tuple or list of tuples (default: [])
            One or several attribute-value[-unit]] tuples to be modified
            for data objects.

        verbose: bool (default: False)
            Whether to print more output.
        """
        if isinstance(iterator, str):
            iterator = self.session.search.iglob(iterator)

        assert action in ['add', 'remove'], 'Unknown action: %s' % action

        if action == 'add':
            log_msg = 'Adding metadata {avu} to {kind} {path}'
        else:
            log_msg = 'Removing metadata {avu} from {kind} {path}'

        if isinstance(object_avu, tuple): object_avu = [object_avu]
        if isinstance(collection_avu, tuple): collection_avu = [collection_avu]

        for item in iterator:
            path = self.session.path.get_absolute_irods_path(item)

            if self.session.collections.exists(path):
                # Item is a collection, not an object
                kind = 'collection'

                if recurse:
                    for avu in collection_avu:
                        self.log(log_msg.format(avu=avu, kind=kind, path=path),
                                 verbose)

                        meta = iRODSMeta(*avu)
                        if action == 'add':
                            self.session.metadata.set(Collection, path, meta)
                        elif action == 'remove':
                            self.session.metadata.remove(Collection, path, meta)

                    self.metadata(item + '/*', action, recurse=True,
                                  collection_avu=collection_avu,
                                  object_avu=object_avu,
                                  verbose=verbose)
                else:
                    self.log('Skipping collection %s (no recursion)' % item,
                             verbose)
            else:
                kind = 'data object'

                for avu in object_avu:
                    self.log(log_msg.format(avu=str(avu), kind=kind, path=path),
                             verbose)

                    meta = iRODSMeta(*avu)
                    if action == 'add':
                        self.session.metadata.set(DataObject, path, meta)
                    elif action == 'remove':
                        self.session.metadata.remove(DataObject, path, meta)

    def add_job_metadata(self, iterator, recurse=False, verbose=False):
        """ Add job-related metadata to selected data objects and collections.

        Examples:

        >>> session.bulk.add_job_metadata('~/data/out*.txt')

        Arguments:

        iterator: iterator or str
            Defines which items are subject to the bulk operation.
            Can be an iterator (e.g. using search_manager.find())
            or a string (which will be used to construct a
            search_manager.iglob() iterator). Job metadata will be added for
            matching data objects and, if used recursively, collections.

        recurse: bool (default: False)
            Whether to use recursion, meaning that job metadata will be
            added to matching collections and their data objects and
            subcollections.

        verbose: bool (default: False)
            Whether to print more output.
        """
        # Gather job-related information from available environment variables
        avus = []
        for key in job_env_var:
            if key in os.environ:
                if key.endswith('FILE'):
                    # e.g. $PBS_NODEFILE is special, as it refers to a file
                    with open(os.environ[key], 'r') as f:
                        value = ','.join([line.strip() for line in f])

                    listkey = key.replace('FILE', 'LIST')
                    avus.append((listkey, value))
                else:
                    avus.append((key, os.environ[key]))

        self.metadata(iterator, action='add',
                      collection_avu=avus,
                      object_avu=avus,
                      recurse=recurse,
                      verbose=verbose)
