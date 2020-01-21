#PBS -S /bin/bash

# Select one node with 
#PBS -l select=87:model=bro
# Submit job to the devel queue
#PBS -q devel
# Send an e-mail on abort
#PBS -m a
#PBS -M heather.cronk@colostate.edu

###Load python
###module load python3/3.7.0

# By default, PBS executes your job from your home directory.
export PBS_O_WORKDIR=~/ditl_1/aggregate
cd $PBS_O_WORKDIR

base_data_dir="/nobackup/hcronk/data/process"
grans=($(ls -d ${base_data_dir}/*))
###printf '%s\n' "${grans[@]}">temp.txt

# Run the test

###echo ${grans[@]} | parallel -j 1 --sshloginfile $PBS_NODEFILE "cd $PWD;python retrieval_aggregation.py {}"
printf '%s\n' "${grans[@]}" | parallel -j 1 --sshloginfile $PBS_NODEFILE "cd $PWD;./parallel_agg_jobs.csh {}"
