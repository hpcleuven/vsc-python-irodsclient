""" Tests of basic VSCiRODSSession functionality

For this purpose, a temporary ".irodstest" collection will be used
inside your iRODS home.
"""

import os
import fnmatch
import tempfile
from vsc_irods.session import VSCiRODSSession


def create_tmpdir(session, tmpdir):
    path = session.path.get_absolute_irods_path(tmpdir)
    assert not session.collections.exists(path)
    session.path.imkdir(tmpdir, verbose=True)
    return


def remove_tmpdir(session, tmpdir):
    path = session.path.get_absolute_irods_path(tmpdir)
    assert session.collections.exists(path)
    session.bulk.remove(tmpdir, recurse=True, force=True, verbose=True)
    return


def test_absolute_paths(session, tmpdir):
    create_tmpdir(session, tmpdir)

    # Check that tmpdir exists
    hits = session.search.glob(tmpdir, debug=True)
    assert hits == [tmpdir]

    # Check that tmpdir is empty
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert len(hits) == 0

    # Check equivalent absolute paths
    hit = session.path.get_absolute_irods_path(tmpdir)
    for template in ['~/%s/', '%s', '%s/', './%s', './%s/', '~/%s/.']:
        d = template % os.path.basename(tmpdir)
        assert hit == session.path.get_absolute_irods_path(d), (hit, d)

    session.path.ichdir(tmpdir)
    for template in ['.', '../%s']:
        d = template % os.path.basename(tmpdir) if '%' in template else template
        assert hit == session.path.get_absolute_irods_path(d), (hit, d)
    session.path.ichdir('~')

    remove_tmpdir(session, tmpdir)
    return


def test_imkdir(session, tmpdir):
    create_tmpdir(session, tmpdir)

    # This is not supposed to work
    try:
        session.path.imkdir(tmpdir, verbose=True)
    except AssertionError:
        pass
    else:
        msg = 'Trying to create an already existing collection is expected '
        msg += 'to raise an AssertionError'
        raise RuntimeError(msg)

    nested_dir = os.path.join(tmpdir, 'nested/directory')

    # This is not supposed to work either
    try:
        session.path.imkdir(nested_dir, parents=False, verbose=True)
    except AssertionError:
        pass
    else:
        msg = 'Trying to create a collection where the parent does not exist '
        msg += 'and recurse=False is expected to raise an AssertionError'
        raise RuntimeError(msg)

    # But these should work though
    session.path.imkdir(nested_dir, parents=True, verbose=True)
    session.path.imkdir(nested_dir, parents=True, verbose=True)

    abs_path = session.path.get_absolute_irods_path(nested_dir)
    assert session.collections.exists(abs_path)

    remove_tmpdir(session, tmpdir)
    return


def test_put(session, tmpdir):
    create_tmpdir(session, tmpdir)

    session.bulk.put('data/README', irods_path=tmpdir, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == ['%s/README' % tmpdir], hits

    session.bulk.put('./data/molecule_names*', irods_path=tmpdir, recurse=True,
                     verbose=True)

    print('> Expecting a message that this transfer will be skipped:')
    session.bulk.put('data/molecule_names.txt', irods_path=tmpdir,
                     clobber=False, recurse=False, verbose=True)

    print('> No such message expected here:')
    session.bulk.put('data/molec*', irods_path=tmpdir, recurse=True,
                     verbose=True)

    hits = session.search.glob(tmpdir + '/molec*', debug=True)
    expected = ['%s/molecules' % tmpdir, '%s/molecule_names.txt' % tmpdir]
    assert len(hits) == len(expected), hits
    assert all([hit in expected for hit in hits]), hits

    hits = session.search.glob(tmpdir + '/molecules/*', debug=True)
    with open('data/molecule_names.txt', 'r') as f:
        for line in f.readlines():
            fname = line.rstrip()
            assert '%s/molecules/%s' % (tmpdir, fname) in hits, (hits, fname)

    remove_tmpdir(session, tmpdir)
    return


def test_remove(session, tmpdir):
    create_tmpdir(session, tmpdir)

    testdir = tmpdir + '/testdir'
    testdir_abs = session.path.get_absolute_irods_path(testdir)
    session.collections.create(testdir_abs)

    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == [testdir], (hits, testdir)

    session.bulk.remove(testdir, force=True, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == [testdir], (hits, testdir)

    session.bulk.remove(testdir, recurse=True, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert len(hits) == 0, hits

    remove_tmpdir(session, tmpdir)
    return


def test_get(session, tmpdir):
    create_tmpdir(session, tmpdir)

    testdir = tmpdir + '/testdir'
    testdir_abs = session.path.get_absolute_irods_path(testdir)
    session.collections.create(testdir_abs)

    testfile = testdir + '/testfile'
    testfile_abs = session.path.get_absolute_irods_path(testfile)
    session.data_objects.create(testfile_abs)

    with tempfile.TemporaryDirectory() as tmpdest:
        print('Using temporary test directory:', tmpdest)
        session.bulk.get(testdir, local_path=tmpdest, recurse=True,
                         clobber=False, verbose=True)
        session.bulk.get(testdir, local_path=tmpdest, recurse=True,
                         clobber=True, verbose=True)
        d = os.path.join(tmpdest, os.path.basename(testdir))
        assert os.path.isdir(d)
        f = os.path.join(d, os.path.basename(testfile))
        assert os.path.isfile(f), f

    remove_tmpdir(session, tmpdir)
    return


def test_find(session, tmpdir):
    create_tmpdir(session, tmpdir)

    session.bulk.put('data', irods_path=tmpdir, recurse=True, verbose=True)
    session.path.ichdir(tmpdir)

    ndepths = 3
    all_refs = {depth: [] for depth in range(ndepths)}
    all_refs[0].append('./data')

    for (folder, subfolder, files) in os.walk('./data'):
        depth = folder.count('/')
        all_refs[depth].extend([os.path.join(folder, f) for f in files])
        all_refs[depth].extend([os.path.join(folder, d) for d in subfolder])

    for depth, refs in all_refs.items():
        print('depth: %s, references: %s' % (depth, refs))

    for mindepth in range(ndepths):
        for maxdepth in list(range(mindepth, ndepths)) + [-1]:
            if maxdepth == -1:
                depths = list(range(mindepth, ndepths))
            else:
                depths = list(range(mindepth, maxdepth + 1))

            print('Checking mindepth = %d, maxdepth = %d, depths = %s' %
                  (mindepth, maxdepth, depths))

            refs = [item for depth in depths for item in all_refs[depth]]

            iterator = session.search.find('./data', mindepth=mindepth,
                                           maxdepth=maxdepth, debug=True)
            n = 0
            for item in iterator:
                assert item in refs, (item, 'not in', refs)
                n += 1
            assert n == len(refs), '%d items are missing!' % (len(refs) - n)

    remove_tmpdir(session, tmpdir)
    return


def test_metadata(session, tmpdir):
    create_tmpdir(session, tmpdir)

    from irods.models import DataObject

    attribute = 'kind'
    values = ['organic', 'inorganic', '%org%']
    operators = ['=', '=', 'like']
    counts = {value: 0 for value in values}

    def generate_value(path):
        is_organic = os.path.basename(path).lower().startswith('c')
        return 'organic' if is_organic else 'inorganic'

    # Upload the molecules to iRODS
    session.bulk.put('data/molecules', irods_path=tmpdir, recurse=True,
                     verbose=True)

    d = os.path.join(tmpdir, 'molecules')

    # Generate and set metadata
    for item in session.search.find(d, '*.xyz', debug=True):
        value = generate_value(item)
        path = session.path.get_absolute_irods_path(item)
        session.bulk.metadata(path, object_avu=(attribute, value),
                              action='add', verbose=True)
        metadata = session.metadata.get(DataObject, path)
        print('Checking metadata for %s: %s' % (item, metadata))
        counts[value] += 1

    assert sum(counts.values()) > 0, counts
    counts['%org%'] = counts['organic'] + counts['inorganic']

    # Test the metadata search filter
    counter_sum = 0

    for value, operator in zip(values, operators):
        object_avu = ('=,%s' % attribute, '%s,%s' % (operator, value))
        pattern = value.replace('%', '*')

        counter = 0

        # Check that find() yields the expected hits
        for item in session.search.find(d, '*.xyz', types='f', debug=True,
                                        object_avu=object_avu):
            counter += 1
            print('Matching attribute "%s" "%s" "%s" for %s' % \
                (attribute, operator, value, item))
            assert fnmatch.fnmatch(generate_value(item), pattern), (item, value)

        assert counts[value] == counter, (value, counts[value], counter)
        counter_sum += counter

        # Test for 'piping' an AVU-containing search into a bulk operation
        iterator = session.search.find(d, '*.xyz', types='f', debug=True,
                                       object_avu=object_avu)
        counter = 0

        for obj in session.bulk.get(iterator, return_data_objects=True,
                                    verbose=True):
            counter += 1
            value_get = obj.metadata.get_one(attribute).value
            assert fnmatch.fnmatch(value_get, pattern), (value_get, value)

        assert counts[value] == counter, (value, counts[value], counter)

    assert counter_sum > 0


    # Removing the metadata
    for value, operator in zip(values[:2], operators[:2]):
        object_avu = ('=,%s' % attribute, '%s,%s' % (operator, value))
        object_avu_remove = (attribute, value)

        iterator = session.search.find(d, '*.xyz', types='f', debug=True,
                                       object_avu=object_avu)

        session.bulk.metadata(iterator, object_avu=object_avu_remove,
                              action='remove', verbose=True)

    # Checking that there are no more AVU matches
    object_avu = ('=,%s' % attribute, '%s,%s' % (operators[2], values[2]))
    iterator = session.search.find(d, '*.xyz', types='f', debug=True,
                                   object_avu=object_avu)

    counter = len([hit for hit in iterator])
    assert counter == 0, counter

    remove_tmpdir(session, tmpdir)
    return


def test_add_job_metadata(session, tmpdir):
    create_tmpdir(session, tmpdir)

    from vsc_irods.manager.bulk_manager import job_env_var

    # Create 'output' file
    f = os.path.join(tmpdir, 'outputfile.txt')
    f_abs = session.path.session.path.get_absolute_irods_path(f)
    session.data_objects.create(f_abs)

    # Generate dummy job environment variables
    filelist = 'abc,def'
    tmp_fhandles = {}

    for key in job_env_var:
        assert key not in os.environ

        if key.endswith('FILE'):
            fhandle = tempfile.NamedTemporaryFile(mode='w')
            print('Using local tmp file %s for key %s' % (fhandle.name, key))

            for line in filelist.split(','):
                fhandle.write(line + '\n')

            fhandle.flush()
            os.environ[key] = fhandle.name
            tmp_fhandles[key] = fhandle
        else:
            os.environ[key] = key.lower()

    # Add them to the output file
    session.bulk.add_job_metadata(f, verbose=True)

    # Check that all job environment variables are present
    obj = session.data_objects.get(f_abs)

    for key in job_env_var:
        print('Checking key', key)

        if key.endswith('FILE'):
            listkey = key.replace('FILE', 'LIST')
            value = obj.metadata.get_one(listkey).value
            assert value == filelist, (value, filelist)
            tmp_fhandles[key].close()
        else:
            value = obj.metadata.get_one(key).value
            assert value == key.lower(), (value, key)

        del os.environ[key]

    remove_tmpdir(session, tmpdir)
    return


def test_size(session, tmpdir):
    create_tmpdir(session, tmpdir)

    session.bulk.put('data', irods_path=tmpdir, recurse=True, verbose=True)
    session.path.ichdir(tmpdir)

    # Check with individual data objects
    iterator = session.search.find('./data', types='f')
    total = 0
    for path, size in session.bulk.size(iterator, verbose=True):
        print('Size of %s in bytes: %d' % (path, size))
        total += size
    assert total == 3277, total

    # Check with the parent collection, non-recursively
    results = [result for result in session.bulk.size('data/', recurse=False,
                                                      verbose=True)]
    assert len(results) == 0, 'Getting the size of a collection in a ' + \
                              'non-recursive manner should give no results'

    # Check with the parent collection, recursively
    results = [result for result in session.bulk.size('data/', recurse=True,
                                                      verbose=True)]
    assert len(results) == 1, 'Getting the size of a collection in a ' + \
                              'non-recursive manner should give 1 result'
    path, size = results[0]
    assert total == size, (total, size)

    remove_tmpdir(session, tmpdir)
    return


def test_move(session, tmpdir):
    create_tmpdir(session, tmpdir)

    from irods.exception import OperationNotSupported

    session.bulk.put('data', irods_path=tmpdir, recurse=True, verbose=True)
    session.path.ichdir(tmpdir)

    try:
        session.bulk.move('data/molecules', 'data/molecule_names.txt',
                          verbose=True)
    except OperationNotSupported:
        pass
    else:
        msg = 'Moving a collection to an existing data object '
        msg += 'is expected to raise an OperationNotSupported error'
        raise RuntimeError(msg)

    # Rename a single data object
    source, dest = 'data/molecule_names.txt', 'data/names.txt'
    session.bulk.move(source, dest, verbose=True)

    source_abs = session.path.get_absolute_irods_path(source)
    assert not session.data_objects.exists(source_abs)

    dest_abs = session.path.get_absolute_irods_path(dest)
    assert session.data_objects.exists(dest_abs)

    # Move all the content to a new collection
    source, dest = 'data', 'new'
    size_0 = sum([size for p, size in session.bulk.size(source, recurse=True)])

    session.path.imkdir(dest)
    session.bulk.move(source + '/*', dest, verbose=True)

    size_1 = sum([size for p, size in session.bulk.size(source, recurse=True)])
    assert size_1 == 0, size_1

    size_2 = sum([size for p, size in session.bulk.size(dest, recurse=True)])
    assert size_0 == size_2, (size_0, size_2)

    remove_tmpdir(session, tmpdir)
    return

def test_case_sensitivity(session, tmpdir):
    # Checks whether avus retain their capital letters when added
    # Assumes the iCAT database is case-sensitive.
    # If this is not the case, feel free to comment this testcase
    # at the end of the script.


    create_tmpdir(session, tmpdir)

    attribute = 'kind' 
    givenValue1 = 'organic'
    givenValue2 = 'Organic'
    message = "The iCAT database is not case-sensitive."
    
    session.bulk.put('data/molecules/ch*', irods_path=tmpdir, verbose=True, recurse=True)

    session.bulk.metadata(tmpdir+"/ch2och2.xyz", object_avu=(attribute, givenValue1),
                         action='add', verbose=True)
    session.bulk.metadata(tmpdir+"/ch3cooh.xyz", object_avu=(attribute, givenValue2),
                         action='add', verbose=True)
 
    obj1 = session.bulk.get(tmpdir+"/ch2och2.xyz", return_data_objects=True, verbose=True)[0]
    obj2 = session.bulk.get(tmpdir+"/ch3cooh.xyz", return_data_objects=True, verbose=True)[0]

    returnedValue1 = obj1.metadata.get_one(attribute).value
    returnedValue2 = obj2.metadata.get_one(attribute).value

    assert returnedValue1 == givenValue1 and returnedValue2 == givenValue2, message

    remove_tmpdir(session, tmpdir)
    return


if __name__ == '__main__':
    with VSCiRODSSession(txt='-') as session:
        tmpdir = '~/.irodstest'
        test_absolute_paths(session, tmpdir)
        test_imkdir(session, tmpdir)
        test_put(session, tmpdir)
        test_remove(session, tmpdir)
        test_get(session, tmpdir)
        test_find(session, tmpdir)
        test_metadata(session, tmpdir)
        test_add_job_metadata(session, tmpdir)
        test_size(session, tmpdir)
        test_move(session, tmpdir)
        test_case_sensitivity(session, tmpdir)
