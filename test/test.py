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
        session.path.imkdir(nested_dir, recurse=False, verbose=True)
    except AssertionError:
        pass
    else:
        msg = 'Trying to create a collection where the parent does not exist '
        msg += 'and recurse=False is expected to raise an AssertionError'
        raise RuntimeError(msg)

    # But this should work though
    session.path.imkdir(nested_dir, recurse=True, verbose=True)

    abs_path = session.path.get_absolute_irods_path(nested_dir)
    assert session.collections.exists(abs_path)

    remove_tmpdir(session, tmpdir)
    return


def test_put(session, tmpdir):
    create_tmpdir(session, tmpdir)

    session.bulk.put('data/README', irods_path=tmpdir, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == ['%s/README' % tmpdir], hits

    session.bulk.put('data/molec*', irods_path=tmpdir, recurse=True,
                     verbose=True)

    session.bulk.put('data/molec*', irods_path=tmpdir, recurse=True,
                     force=True, verbose=True)

    hits = session.search.glob(tmpdir + '/molec*', debug=True)
    expected = ['%s/molecules' % tmpdir, '%s/molecule_names.txt' % tmpdir]
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
                         verbose=True)
        session.bulk.get(testdir, local_path=tmpdest, recurse=True,
                         force=True, verbose=True)
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

    references = []
    for (folder, subfolder, files) in os.walk('./data'):
        references.extend([os.path.join(folder, f) for f in files])
        references.extend([os.path.join(folder, d) for d in subfolder])

    n = 0
    for item in session.search.find('./data', debug=False):
        assert references.count(item) == 1, (item, 'not in', references)
        n += 1
    assert n == len(references), '%d items are missing!' % (len(references) - n)

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
            key = attribute.title()
            value_get = obj.metadata.get_one(key).value
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
