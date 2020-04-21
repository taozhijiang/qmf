
# Usage example

1. local standalone version, for golden   
```bash
~ # generate seed
~ bin/gen_uniform 100000000
~
~ # small example
~ bin/wals -nfactors=30 -train_dataset=./n_rating.csv -distribution_file=./uniform.dat -user_factors=./user.dat -item_factors=item.dat
~
~ # big example
~ bin/wals -nfactors=100 -train_dataset=./n_rating2.csv -distribution_file=./uniform.dat -user_factors=./user2.dat -item_factors=item2.dat
```

2. distributed version   
use GLOG_v=3 if you want to seen verbose information.   
```bash
~ # reuse the seed
~
~ # startup Scheduler
~ bin/wals_scheduler
~
~ # startup multi-Labors
~ bin/wals_labor
~ bin/wals_labor
...
~
~ # small task example
~ bin/wals_submit 127.0.0.1 8900 ../task.pb
~
~ # big task example
~ bin/wals_submit 127.0.0.1 8900 ../task.pb
```