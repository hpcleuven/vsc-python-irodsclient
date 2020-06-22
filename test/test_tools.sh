# Basic tests of the command line scripts in /tools/

# Change to iRODS home
icd

# Make collection for testing purposes
tmpdir=".irodstest"
echo "Making test collection "$tmpdir
imkdir $tmpdir
icd $tmpdir
irods_path="~/"$tmpdir

# Copy content to iRODS
echo "TEST: vsc-prc-iput"
vsc-prc-iput -r ../test/data* -d $irods_path --verbose
echo "iRODS tmpdir content:"
ils -r .

# Fetch content to local directory
echo "TEST: vsc-prc-iget"
local_tmpdir=`mktemp -d`
echo "Local tmpdir path: "$local_tmpdir
vsc-prc-iget -r $irods_path"/data/molecules/" -d $local_tmpdir --verbose
echo "Local tmpdir content: "
find $local_tmpdir
echo "Removing local tmpdir "$local_tmpdir
rm -r $local_tmpdir

# Add metadata to certain files
echo "TEST: vsc-prc-imeta (add)"
vsc-prc-imeta $irods_path"/data/molec*/c*.xyz" --object_avu=Kind,organic \
           --action=add --verbose

# Try to find these files using vsc-prc-find with an AVU filter
echo "TEST: vsc-prc-find"
vsc-prc-find $irods_path"/data/molecules/" --object_avu="=,kind;like,%org%" \
            --types=f --debug

# Remove that same metadata
echo "TEST: vsc-prc-imeta (remove)"
vsc-prc-imeta $irods_path"/data/molec*/c*.xyz" --object_avu=Kind,organic \
           --action=remove --verbose

# Remove all the uploaded content
echo "TEST: vsc-prc-irm"
vsc-prc-irm -r $irods_path"/data/" --verbose

# Remove the now-empty test collection
echo "Removing test collection "$tmpdir
icd
irmdir $tmpdir
