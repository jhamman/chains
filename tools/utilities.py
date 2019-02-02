import getpass
import os
import time


# utilities
def make_case_readme(combo, fname, **kwargs):
    if os.path.isfile(fname):
        return
    user = getpass.getuser()
    now = time.ctime(time.time())
    title = '-'.join(combo)
    log = '{now}: \tCase directory and readme created by {user}'.format(
        user=user, now=now)

    readme = '''# {title}
---------

### Methods in this ensemble:
  - DISAGG_METHODS: {disagg_methods}
  - HYDRO_METHODS: {hydro_methods}
  - ROUTING_METHODS: {routing_methods}

### Log
{log}
'''

    with open(fname, 'w') as f:
        f.write(readme.format(title=title, log=log, **kwargs))


def log_to_readme(message, fname):
    now = time.ctime(time.time())
    log = '{now}: \t{message}\n'.format(message=message, now=now)

    with open(fname, 'a') as f:
        f.write(log)
