import os
import nbformat
from nbconvert import PythonExporter

py = PythonExporter()

githubBase = os.path.expanduser('~/github/etcbc')
programDir = 'programs'
standardParams = 'CORE_NAME VERSION CORE_MODULE'

def mustRun(force):
    def _mustRun(fileIn, fileOut):
        xFileIn = None if fileIn == None else os.path.exists(fileIn)
        xFileOut = os.path.exists(fileOut)
        tFileIn = os.path.getmtime(fileIn) if xFileIn else None
        tFileOut = os.path.getmtime(fileOut) if xFileOut else None
        good = True
        work = True
        if fileIn == None:
            if xFileOut:
                print('\tDestination {} exists'.format(fileOut))
                work = False
            else:
                print('\tDestination {} does not exist'.format(fileOut))
                work = True
        elif xFileIn:
            print('\tSource {} exists'.format(fileIn))
            if xFileOut:
                print('\tDestination {} exists'.format(fileOut))
                if tFileOut >= tFileIn:
                    print('\tDestination {} up to date'.format(fileOut))
                    work = False
                else:
                    print('\tDestination {} is outdated'.format(fileOut))
            else:
                print('\tDestination {} does not exist'.format(fileOut))
        else:
            print('\tSource {} does not exist'.format(fileIn))
            if xFileOut:
                print('\tDestination {} exists'.format(fileOut))
                print('\tDestination {} counts as up to date'.format(fileOut))
                work = False
            else:
                print('\tDestination {} does not exist'.format(fileOut))
                print('\tDestination {} cannot be made: source is missing'.format(fileOut))
                good = False
                work = False
        return (good, work or force)
    return _mustRun

def runNb(repo, dirName, nb, force=False, **parameters):
    location = '{}/{}/{}'.format(githubBase, repo, dirName)
    nbFile = '{}/{}.ipynb'.format(location, nb)
    pyFile = '{}/{}.py'.format(location, nb)
    nbObj = nbformat.read(nbFile, 4)
    pyScript = py.from_notebook_node(nbObj)[0]
    with open(pyFile, 'w') as s: s.write(pyScript)
    os.chdir(location)
    good = True
    with open(pyFile) as s:
        locals()['SCRIPT'] = True
        locals()['MUSTRUN'] = mustRun(force)
        for (param, value) in parameters.items():
            locals()[param] = value
        print('START {} ({})'.format(
            nb,
            ', '.join('{}={}'.format(*p) for p in sorted(parameters.items())),
        ))
        try:
            exec(s.read(), locals())
        except SystemExit as inst:
            good = inst.args[0] == 0
        print('{} {}'.format('SUCCESS' if good else 'FAILURE', nb))

    return good

def checkRepo(repo, repoConfig, force=False, **parameters):
    good = True
    for item in repoConfig:
        task = item.get('task', None)
        if task == None:
            print('ERROR: missing task name in item {}'.format(item))
            good = False

        paramNames = (standardParams + ' ' + item.get('params', '')).strip().split() 
        for param in paramNames:
            if param not in parameters:
                print('ERROR: {} needs parameter {} which is not supplied'.format(
                    task, param,
                ))
                good = False
    return good

def runRepo(repo, repoConfig, force=False, **parameters):
    good = True
    for item in repoConfig:
        task = item['task']
        paramNames = (standardParams + ' ' + item.get('params', '')).strip().split() 
        paramValues = dict()
        for param in paramNames:
            paramValues[param] = parameters[param]

        good = runNb(repo, programDir, task, force=force, **paramValues)
        if not good: break
    return good

def runRepos(repoOrder, repoConfig, force=False, **parameters):
    good = True
    for repo in repoOrder.strip().split():
        if repo not in repoConfig:
            print('ERROR: missing configuration for repo {}'.format(repo))
            good = False
        if not checkRepo(repo, repoConfig[repo], **parameters):
            good = False
    if not good: return False

    for repo in repoOrder.strip().split():
        good = runRepo(repo, repoConfig[repo], force=force, **parameters)
        if not good: break
    return good

def runPipeline(pipeline, version=None, force=False):
    good = True
    for key in ('defaults', 'versions', 'repoOrder', 'repoConfig'):
        if key not in pipeline:
            if key == 'defaults':
                if version == None:
                    print('ERROR: no {} version given and no known default section in pipeline')
                    good = False
            else:
                print('ERROR: no {} declared in the pipeline'.format(key))
                good = False
        elif key == 'defaults':
            if version == None:
                if 'VERSION' not in pipeline['defaults']:
                    print('ERROR: no version given and no default version specified in pipeline')
                    good = False
                else:
                    version = pipeline['defaults']['VERSION']
        elif key == 'versions':
            if version not in pipeline['versions']:
                if version != None:
                    print('ERROR: version {} not declared in pipeline'.format(version))
                good = False
            else:
                versionInfo = pipeline['versions'][version]
    if not good:
        return False
    defaults = pipeline.get('defaults', {})
    versions = pipeline['versions']
    repoOrder = pipeline['repoOrder']
    repoConfig = pipeline['repoConfig']

    versionInfo = pipeline['versions'][version]
    paramValues = dict()
    for param in standardParams.strip().split():
        if param == 'VERSION':
            value = version
        else:
            value = versionInfo.get(param, defaults.get(param, None))
        if value == None:
            print('ERROR: no value or default value for {}'.format(param))
            good = False
        else:
            paramValues[param] = value
    if not good:
        return False

    print('Going to run pipeline for version {}'.format(version))
    
    return runRepos(repoOrder, repoConfig, force=force, **paramValues)


