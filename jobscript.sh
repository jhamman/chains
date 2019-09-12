#!/bin/sh
set -e
set -x

export PATH=/glade/u/home/jhamman/miniconda3/bin:$PATH

source activate chains
unset LD_LIBRARY_PATH
export LANG="en_US.utf8"
export LANGUAGE="en_US.utf8"
export LC_ALL="en_US.utf8"
export OMP_NUM_THREADS=1
export HDF5_USE_FILE_LOCKING="FALSE"
echo "printing environment"
env
echo "done printing environment"
echo "Hostname: `hostname`"


# properties = {properties}
echo "starting job...`date`"
time {exec_job}
echo "completed job...`date`"

