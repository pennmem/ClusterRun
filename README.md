This repository contains convenience functions and usage examples for
ipython-cluster-helper, providing the ability to quickly switch between SGE and
slurm.

Example:
```python
from ClusterRun import *

settings_file = 'my_analysis.pkl'
settings = Settings()
settings.scheduler = 'slurm'  # 'slurm' or 'SGE'
settings.max_jobs = 16
settings.mem = '20GB'
settings.Save(settings_file)

def RunMyAnalysis(sub):
  try:
    x = 1+1
    # Save results
  except Exception as e:
    # Log error
    return False
  return True

subs = ['R1642J', 'R1644T', 'R1646T']
ClusterChecked(RunMyAnalysis, subs, settings=settings)
```

