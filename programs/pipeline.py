import os
from shutil import copy
import nbformat
from nbconvert import PythonExporter
from tf.fabric import Fabric

from utils import *

py = PythonExporter()

githubBase = os.path.expanduser('~/github/etcbc')
pipelineRepo = 'pipeline'
utilsScript = 'programs/utils.py'

programDir = 'programs'
standardParams = 'CORE_NAME VERSION'.strip().split()


def runNb(repo, dirName, nb, force=False, **parameters):
    caption(3, 'Run notebook [{}/{}]'.format(repo, nb))
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
        locals()['FORCE'] = force
        locals()['NAME'] = repo
        for (param, value) in parameters.items():
            locals()[param] = value
        caption(0, 'START {} ({})'.format(
            nb,
            ', '.join('{}={}'.format(*p) for p in sorted(parameters.items())),
        ))
        try:
            exec(s.read(), locals())
        except SystemExit as inst:
            good = inst.args[0] == 0
        caption(0, '{} {}'.format('SUCCESS' if good else 'FAILURE', nb))

    caption(3, '[{}/{}]'.format(repo, nb), good=good)
    return good

def checkRepo(repo, repoConfig, force=False, **parameters):
    good = True
    for item in repoConfig:
        task = item.get('task', None)
        if task == None:
            caption(0, 'ERROR: missing task name in item {}'.format(item))
            good = False

        for param in standardParams:
            if param not in parameters:
                caption(0, 'ERROR: {} needs parameter {} which is not supplied'.format(
                    task, param,
                ))
                good = False
    return good

def runRepo(repo, repoConfig, force=False, **parameters):
    caption(2, 'Make repo [{}]'.format(repo))
    # copy the utils.py from the pipeline repo to the target repo
    copy(
        '{}/{}/{}'.format(githubBase, pipelineRepo, utilsScript),
        '{}/{}/{}'.format(githubBase, repo, utilsScript),
    )
    good = True
    for item in repoConfig:
        task = item['task']
        omit = item.get('omit', set())
        paramValues = dict()
        for param in standardParams:
            paramValues[param] = parameters[param]
        if 'params' in item:
            paramValues.update(item['params'])
        version = paramValues.get('VERSION', 'UNKNOWN')
        if version in omit:
            caption(3, '[{}/{}] skipped in version [{}]'.format(repo, task, version))
            continue

        good = runNb(repo, programDir, task, force=force, **paramValues)
        if not good: break
    caption(2, '[{}]'.format(repo), good=good)
    return good

def runRepos(repoOrder, repoConfig, force=False, **parameters):
    good = True
    for repo in repoOrder.strip().split():
        if repo not in repoConfig:
            caption(0, 'ERROR: missing configuration for repo {}'.format(repo))
            good = False
        if not checkRepo(repo, repoConfig[repo], **parameters):
            good = False
    if not good: return False

    for repo in repoOrder.strip().split():
        good = runRepo(repo, repoConfig[repo], force=force, **parameters)
        if not good: break
    return good

def runPipeline(pipeline, version=None, force=False):
    caption(1, 'Make version [{}]'.format(version))
    
    good = True
    for key in ('defaults', 'versions', 'repoOrder', 'repoConfig'):
        if key not in pipeline:
            if key == 'defaults':
                if version == None:
                    caption(0, 'ERROR: no {} version given and no known default section in pipeline')
                    good = False
            else:
                caption(0, 'ERROR: no {} declared in the pipeline'.format(key))
                good = False
        elif key == 'defaults':
            if version == None:
                if 'VERSION' not in pipeline['defaults']:
                    caption(0, 'ERROR: no version given and no default version specified in pipeline')
                    good = False
                else:
                    version = pipeline['defaults']['VERSION']
        elif key == 'versions':
            if version not in pipeline['versions']:
                if version != None:
                    caption(0, 'ERROR: version {} not declared in pipeline'.format(version))
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
    for param in standardParams:
        if param == 'VERSION':
            value = version
        else:
            value = versionInfo.get(param, defaults.get(param, None))
        if value == None:
            caption(0, 'ERROR: no value or default value for {}'.format(param))
            good = False
        else:
            paramValues[param] = value
    if not good:
        return False

    good = runRepos(repoOrder, repoConfig, force=force, **paramValues)
    caption(1, '[{}]'.format(version), good=good)
    return good

def webPipeline(pipeline, version, force=False):
    caption(1, 'Aggregate MLQ for version {}'.format(version))
    good = True
    for key in ('repoOrder', 'coreModule'):
        if key not in pipeline:
            caption(0, '\tERROR: no {} declared in the pipeline'.format(key))
            good = False
    if not good:
        return False

    repoOrder = pipeline['repoOrder'].strip().split()
    coreModule = pipeline['coreModule']

    resultRepo = repoOrder[0]
    addedRepos = repoOrder[1:]

    tempDir = '{}/{}/_temp/{}'.format(githubBase, resultRepo, version)
    shebanqDir = '{}/{}/shebanq/{}'.format(githubBase, resultRepo, version)

    dbName = '{}_xx'.format(resultRepo)

    mqlUFile = '{}/{}.mql'.format(tempDir, dbName)
    mqlZFile = '{}/{}.mql.bz2'.format(shebanqDir, dbName)

    xmU = os.path.exists(mqlUFile)
    xmZ = os.path.exists(mqlZFile)

    uptodate = True

    referenceFile = mqlUFile if xmU else mqlZfile

    if not os.path.exists(referenceFile):
        uptodate = False
        caption(0, '\tWork to do because {} does not exist'.format(referenceFile))
    else:
        tmR = os.path.getmtime(referenceFile)
        for (i, repo) in enumerate(repoOrder):
            module = repo if i else coreModule
            tfxDir = '{}/{}/tf/{}/{}/.tf'.format(githubBase, repo, version, module)
            if not os.path.exists(tfxDir):
                uptodate = False
                caption(0, '\tWork to do because the tf in {} is fresh'.format(repo))
                caption(0, '\t\t{}'.format(tfxDir))
                break
            if os.path.getmtime(tfxDir) > tmR:
                uptodate = False
                caption(0, '\tWork to do because the tf in {} is recently compiled'.format(repo))
                caption(0, '\t\t{}'.format(tfxDir))
                break

    if uptodate and force:
        caption(0, '\tWork to do because you forced me to!')
        uptodate = False
    if not uptodate:
        caption(1, 'Using TF to make an MQL export')
        locations = []
        modules = []
        for (i, repo) in enumerate(repoOrder):
            module = repo if i else coreModule
            locations.append('{}/{}/tf/{}'.format(githubBase, repo, version))
            modules.append(module)

        TF = Fabric(locations=locations, modules=modules) 
        TF.exportMQL(dbName, tempDir)
    else:
        caption(0, '\tAlready up to date')

    caption(0, '\tbzipping {}'.format(mqlUFile))
    caption(0, '\tand delivering as {}'.format(mqlZFile))
    bzip(mqlUFile, mqlZFile)

