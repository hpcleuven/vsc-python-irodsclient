#!/bin/bash
#PBS -l walltime=00:05:00
#PBS -l nodes=1:ppn=1
#PBS -N vsc_prc_test_pbs

# This jobscript illustrates how the VSC-PRC command line tools
# can be used in a jobscript (here for a PBS-like resource manager)

# First, create an iRODS collection with data for this example
# In regular workflows, such a collection will already exist
tmpdir="~/.irodstest"
vsc-prc-imkdir $tmpdir
vsc-prc-iput -r $VSC_PRC_ROOT/test/data/molecules --destination=$tmpdir
echo "Contents of $tmpdir:"
ils $tmpdir

# Make a scratch folder on the local file system
scratchdir=$VSC_SCRATCH/$PBS_JOBID
mkdir $scratchdir
cd $scratchdir

# Get sample input files from iRODS
vsc-prc-iget "$tmpdir/molecules/*.xyz" --destination="." --verbose

# Do something with these files; to keep this example very simple,
# we will just write the output of 'wc -l' of each <molecule>.xyz
# to a file <molecule>.out
filenames=`find . -name "*.xyz"`
for filename in $filenames; do
  name=$(basename "$filename" | cut -f 1 -d '.')
  wc -l $filename > $name".out"
done

# Upload the output files to iRODS
vsc-prc-iput *.out --destination="$tmpdir/molecules" --verbose

# Add job metadata to these output files
vsc-prc-add-job-metadata "$tmpdir/molecules/*.out" --verbose

# Remove the scratch directory
cd; rm -r $scratchdir

# Remove the iRODS collection for this example
# In regular workflows, this should not be done
vsc-prc-irm -r $tmpdir --verbose
