#!/bin/csh -f

# Load the compiler used to compile
source ~/.cshrc
module load python3/3.7.0

### Get Variables ###
echo $argv[1]
set gran=$argv[1]

cd ~/ditl_1/aggregate
python retrieval_aggregation.py ${gran}
