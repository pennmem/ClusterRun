# Created 2019-2023 by Ryan A. Colyer
#
# Convenience functions and usage examples for ipython-cluster-helper
# providing the ability to quickly switch between SGE and slurm.
#
# Example:
#   settings_file = 'my_analysis.pkl'
#   settings = Settings()
#   settings.scheduler = 'slurm'  # 'slurm' or 'SGE'
#   settings.max_jobs = 16
#   settings.mem = '20GB'
#   settings.Save(settings_file)
#
#   def RunMyAnalysis(sub):
#     try:
#       x = 1+1
#       # Save results
#     except Exception as e:
#       # Log error
#       return False
#     return True
#
#   subs = ['R1642J', 'R1644T', 'R1646T']
#   ClusterChecked(RunMyAnalysis, subs, settings=settings)



class Settings():
  '''settings = Settings()
     settings.somelist = [1, 2, 3]
     settings.importantstring = 'saveme'
     settings.Save()

     settings = Settings.Load()
  '''
  def __init__(self, **kwargs):
    for k,v in kwargs.items():
      self.__dict__[k] = v

  def Save(self, filename='settings.pkl'):
    import pickle
    with open(filename, 'wb') as fw:
      fw.write(pickle.dumps(self))

  def Load(filename='settings.pkl'):
    import pickle
    return pickle.load(open(filename, 'rb'))

  def __repr__(self):
    return ('Settings(' +
      ', '.join(str(k)+'='+repr(v) for k,v in self.__dict__.items()) +
      ')')

  def __str__(self):
    return '\n'.join(str(k)+': '+str(v) for k,v in self.__dict__.items())


def ClusterRunSGE(function, parameter_list, max_jobs=100, cores_per_job=1):
  '''function: The routine run in parallel, which must contain all necessary
     imports internally.

     parameter_list: should be an iterable of elements, for which each
     element will be passed as the parameter to function for each parallel
     execution.

     max_jobs: Standard Rhino cluster etiquette is to stay within 100 jobs
     running at a time.  Please ask for permission before using more.

     In jupyterlab, the number of engines reported as initially running may
     be smaller than the number actually running.  Check usage from an ssh
     terminal using:  qstat -f | egrep "$USER|node" | less

     Undesired running jobs can be killed by reading the JOBID at the left
     of that qstat command, then doing:  qdel JOBID
  '''
  import cluster_helper.cluster
  from pathlib import Path

  num_jobs = len(parameter_list)
  num_jobs = min(num_jobs, max_jobs)

  myhomedir = str(Path.home())

  with cluster_helper.cluster.cluster_view(scheduler="sge", queue="RAM.q", \
      num_jobs=num_jobs, cores_per_job=cores_per_job, \
      extra_params={'resources':'pename=python-round-robin'}, \
      profile=myhomedir + '/.ipython/') \
      as view:
    # 'map' applies a function to each value within an interable
    res = view.map(function, parameter_list)

  return res


def ClusterRunSlurm(function, parameter_list, max_jobs=64, cores_per_job=1,
    mem='5GB'):
  '''function: The routine run in parallel, which must contain all necessary
     imports internally.

     parameter_list: should be an iterable of elements, for which each
     element will be passed as the parameter to function for each parallel
     execution.

     max_jobs: Standard Rhino cluster etiquette is to stay within 100 jobs
     running at a time.  Please ask for permission before using more.

     cores_per_job: The number of processor cores to allocate per job.

     mem: A string specifying the amount of RAM required per job, formatted
     like '5GB'.

     In jupyterlab, the number of engines reported as initially running may
     be smaller than the number actually running.  Check usage from an ssh
     terminal using:  "squeue" or "squeue -u $USER"

     Undesired running jobs can be killed by reading the JOBID at the left
     of that squeue command, then doing:  scancel JOBID
  '''
  import cluster_helper.cluster
  from pathlib import Path
  import os

  num_jobs = len(parameter_list)
  num_jobs = min(num_jobs, max_jobs)

  myhomedir = str(Path.home())

  with cluster_helper.cluster.cluster_view(scheduler="slurm", queue="RAM", \
      num_jobs=num_jobs, cores_per_job=cores_per_job,
      extra_params={'resources':
        f'account={os.environ["USER"]};timelimit=7-00:00:00;mem={mem}'},
      profile=myhomedir + '/.ipython/') \
      as view:
    # 'map' applies a function to each value within an interable
    res = view.map(function, parameter_list)

  return res


def ClusterRun(*args, **kwargs):
  mapping = {'sge':ClusterRunSGE, 'slurm':ClusterRunSlurm}
  dispatch = mapping['sge']
  max_jobs = 100
  cores_per_job = 1
  mem = '5GB'

  if 'settings' in kwargs:
    dispatch = mapping[kwargs['settings'].scheduler]
    if 'max_jobs' in kwargs['settings'].__dict__:
      max_jobs = kwargs['settings'].max_jobs
    if 'cores_per_job' in kwargs['settings'].__dict__:
      cores_per_job = kwargs['settings'].cores_per_job
    if 'mem' in kwargs['settings'].__dict__:
      mem = kwargs['settings'].mem
    kwargs.pop('settings')

  if len(args) > 2:
    max_jobs = args[2]
  elif 'max_jobs' in kwargs:
    max_jobs = kwargs['max_jobs']
    kwargs.pop('max_jobs')
  if len(args) > 3:
    cores_per_job = args[3]
  elif 'cores_per_job' in kwargs:
    cores_per_job = kwargs['cores_per_job']
    kwargs.pop('cores_per_job')
  if len(args) > 4:
    mem = args[4]
  elif 'mem' in kwargs:
    mem = kwargs['mem']
    kwargs.pop('mem')

  if dispatch == mapping['sge']:
    return dispatch(*args[0:2], max_jobs=max_jobs,
        cores_per_job=cores_per_job, **kwargs)
  else:
    return dispatch(*args[0:2], max_jobs=max_jobs,
        cores_per_job=cores_per_job, mem=mem, **kwargs)


def ClusterChecked(function, parameter_list, *args, **kwargs):
  '''Calls ClusterRun and raises an exception if any results return False.'''
  res = ClusterRun(function, parameter_list, *args, **kwargs)
  if all(res):
    print('All', len(res), 'jobs successful.')
  else:
    failed = sum([not bool(b) for b in res])
    if failed == len(res):
      raise RuntimeError('All '+str(failed)+' jobs failed!')
    else:
      print('Error on job parameters:\n  ' + 
          '\n  '.join(str(parameter_list[i]) for i in range(len(res))
            if not bool(res[i])))
      raise RuntimeError(str(failed)+' of '+str(len(res))+' jobs failed!')

def ClusterCheckedSGE(function, parameter_list, *args, **kwargs):
  '''Calls ClusterRunSGE and raises an exception if any results return
     False.'''
  settings = kwargs.get('settings', Settings())
  settings.scheduler = 'sge'
  kwargs['settings'] = settings
  ClusterChecked(function, parameter_list, *args, **kwargs)

def ClusterCheckedSlurm(function, parameter_list, *args, **kwargs):
  '''Calls ClusterRunSlurm and raises an exception if any results return
     False.'''
  settings = kwargs.get('settings', Settings())
  settings.scheduler = 'slurm'
  kwargs['settings'] = settings
  ClusterChecked(function, parameter_list, *args, **kwargs)

