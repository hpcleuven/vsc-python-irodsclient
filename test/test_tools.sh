# Basic tests of the command line scripts in /tools/

# Change to iRODS home
icd

# Make collection for testing purposes
tmpdir=".irodstest"
irods_path="~/"$tmpdir
echo "Making test collection "$irods_path
vsc-prc-imkdir $irods_path
icd $tmpdir

# Copy content to iRODS
echo "TEST: vsc-prc-iput"
vsc-prc-iput -r ../test/data* -d $irods_path --verbose
echo "iRODS tmpdir content:"
ils -r .

# Move items around
echo "TEST: vsc-prc-imv"
vsc-prc-imv $irods_path"/data/molecules" $irods_path"/data/newname" --verbose
vsc-prc-imv $irods_path"/data/newname/*.xyz" $irods_path"/data/" --verbose
vsc-prc-imv $irods_path"/data/newname" $irods_path"/data/molecules" --verbose
vsc-prc-imv $irods_path"/data/*.xyz" $irods_path"/data/molecules" --verbose

# Get disk usage
echo "TEST: vsc-prc-size"
vsc-prc-size $irods_path -r -H --verbose

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
vsc-prc-find $irods_path"/data/molecules/" --name="c*.xyz" \
             --object_avu="kind;like,%org%" --types=f --debug

# Remove that same metadata
echo "TEST: vsc-prc-imeta (remove)"
vsc-prc-imeta $irods_path"/data/molec*/c*.xyz" --object_avu=Kind,organic \
           --action=remove --verbose

# Add dummy job metadata
echo "TEST: vsc-prc-add-job-metadata"
export PBS_JOBID=666
export PBS_O_HOST=login1
export PBS_JOBNAME=dummyjob
export PBS_NODEFILE=`mktemp`
echo r012345 >> $PBS_NODEFILE
echo r678910 >> $PBS_NODEFILE
vsc-prc-add-job-metadata $irods_path"/data/README" --recurse --verbose
rm $PBS_NODEFILE

# Remove all the uploaded content
echo "TEST: vsc-prc-irm"
vsc-prc-irm -r $irods_path"/data/" --verbose

# Remove the now-empty test collection
echo "Removing test collection "$tmpdir
icd
irmdir $tmpdir
