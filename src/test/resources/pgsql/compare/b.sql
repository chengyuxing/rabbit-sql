[37m/*[ooooooooo]*/[0m[0m
[35mselect[0m [94mcount[0m([93m*[0m),
[94mcount[0m([93m*[0m) [35mfilter[0m ( [35mwhere[0m grade > [36m90[0m )               greate,
[37m       -- #if :_databaseId != blank[0m
[94mcount[0m([93m*[0m) [35mfilter[0m ( [35mwhere[0m grade < [36m90[0m [35mand[0m grade > [36m60[0m) good,
[94m[37m -- #fi[0m
[94mcount[0m([93m*[0m) [35mfilter[0m ( [35mwhere[0m grade < [36m60[0m )               bad
[35mfrom[0m test.score;