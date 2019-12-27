#PBS -S /bin/bash

# Select one node with 
#PBS -l select=1:ncpus=1:mpiprocs=1:model=bro
# Submit job to the devel queue
#PBS -q devel
# Send an e-mail on abort
#PBS -m a
#PBS -M heather.cronk@colostate.edu

#Load python
module load python3/3.7.0

# By default, PBS executes your job from your home directory.
export PBS_O_WORKDIR=~/ditl_1/aggregate
cd $PBS_O_WORKDIR

# Run the test
python retrieval_aggregation.py> ~/ditl_1/aggregate/test_agg_on_compute_nodes.log
