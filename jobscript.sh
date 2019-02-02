#!/bin/sh
set -e
set -x

export PATH=/glade/u/home/jhamman/miniconda3/bin:$PATH

source activate storylines
unset LD_LIBRARY_PATH
export LANG="en_US.utf8"
export LANGUAGE="en_US.utf8"
export LC_ALL="en_US.utf8"
export OMP_NUM_THREADS=1
echo "printing environment"
env
echo "done printing environment"

# properties = {properties}
{exec_job}
