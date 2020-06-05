""" Tests of basic VSCiRODSSession functionality

Assumes that there exists an empty "tmp" collection in your iRODS home,
which will be used for the purpose of testing.
"""

import os
import tempfile
from vsc_irods.session import VSCiRODSSession


def clean_tmpdir(session, tmpdir='~/tmp'):
    path = session.path.get_absolute_irods_path(tmpdir)
    session.collections.remove(path, recurse=True)
    session.collections.create(path, recurse=True)


def test_absolute_paths(session, tmpdir='~/tmp'):
    clean_tmpdir(session, tmpdir=tmpdir)

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


def test_put(session, tmpdir='~/tmp'):
    clean_tmpdir(session, tmpdir=tmpdir)

    session.bulk.put('data/README', irods_path=tmpdir, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == ['%s/README' % tmpdir], hits

    session.bulk.put('data/molec*', irods_path=tmpdir, recurse=True,
                     verbose=True)
    hits = session.search.glob(tmpdir + '/molec*', debug=True)
    expected = ['%s/molecules' % tmpdir, '%s/molecule_names.txt' % tmpdir]
    assert all([hit in expected for hit in hits]), hits

    hits = session.search.glob(tmpdir + '/molecules/*', debug=True)
    with open('data/molecule_names.txt', 'r') as f:
        for line in f.readlines():
            fname = line.rstrip()
            assert '%s/molecules/%s' % (tmpdir, fname) in hits, (hits, fname)


def test_remove(session, tmpdir='~/tmp'):
    clean_tmpdir(session, tmpdir=tmpdir)

    testdir = tmpdir + '/testdir'
    testdir_abs = session.path.get_absolute_irods_path(testdir)
    session.collections.create(testdir_abs)

    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == [testdir], (hits, testdir)

    session.bulk.remove(testdir, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert hits == [testdir], (hits, testdir)

    session.bulk.remove(testdir, recurse=True, verbose=True)
    hits = session.search.glob(tmpdir + '/*', debug=True)
    assert len(hits) == 0, hits


def test_get(session, tmpdir='~/tmp'):
    clean_tmpdir(session, tmpdir=tmpdir)

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
        d = os.path.join(tmpdest, os.path.basename(testdir))
        assert os.path.isdir(d)
        f = os.path.join(d, os.path.basename(testfile))
        assert os.path.isfile(f), f


if __name__ == '__main__':
    with VSCiRODSSession(txt='-') as session:
        test_absolute_paths(session)
        test_put(session)
        test_remove(session)
        test_get(session)
        clean_tmpdir(session)
