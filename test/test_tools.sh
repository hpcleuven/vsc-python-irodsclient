# Basic tests of the command line scripts in /tools/

# Change to iRODS home directory
icd

# Make collection for testing purposes
tmpdir=".irodstest"
echo "Making test directory "$tmpdir
imkdir $tmpdir
icd $tmpdir
irods_path="~/"$tmpdir

# Copy content to iRODS
echo "TEST: bulk-iput"
bulk-iput -r ../test/data* -d $irods_path --verbose
echo "iRODS tmpdir content:"
ils -r .

# Fetch content to local directory
echo "TEST: bulk-iget"
local_tmpdir=`mktemp -d`
echo "Local tmpdir path: "$local_tmpdir
bulk-iget -r $irods_path"/data/molecules/" -d $local_tmpdir --verbose
echo "Local tmpdir content: "
find $local_tmpdir
echo "Removing local tmpdir "$local_tmpdir
rm -r $local_tmpdir

# Add and remove metadata to certain files ...
echo "TEST: bulk-imeta (add)"
bulk-imeta $irods_path"/data/molec*/c*.xyz" --object_avu=Kind,organic \
           --action=add --verbose

echo "TEST: bulk-imeta (remove)"
bulk-imeta $irods_path"/data/molec*/c*.xyz" --object_avu=Kind,organic \
           --action=remove --verbose

# Remove all the uploaded content
echo "TEST: bulk-irm"
bulk-irm -r $irods_path"/data/" --verbose

# Remove the now-empty test directory
echo "Removing test directory "$tmpdir
icd
irmdir $tmpdir
