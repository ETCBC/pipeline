import os
from shutil import copy, copytree, rmtree
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
    caption(3, 'Run notebook [{}/{}] with parameters:'.format(repo, nb))
    for (param, value) in sorted(parameters.items()):
        caption(0, '\t{:<20} = {}'.format(param, value))

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
        for (param, values) in parameters.items():
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

def runVersion(pipeline, version=None, force=False):
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
    for (param, value) in defaults.items():
        if param in standardParams: continue
        paramValues[param] = value
    for (param, value) in versionInfo.items():
        if param in standardParams: continue
        paramValues[param] = value
    if not good:
        return False

    good = runRepos(repoOrder, repoConfig, force=force, **paramValues)
    caption(1, '[{}]'.format(version), good=good)
    return good

def runPipeline(pipeline, versions=None, force=False):
    good = True
    chosenVersions = [] if versions == None else [versions] if type(versions) is str else versions 
    for version in chosenVersions:
        thisGood = runVersion(pipeline, version=version, force=force)
        if not thisGood: good = False
    return good

def copyVersion(pipeline, fromVersion, toVersion):
    caption(1, 'Copy version {} ==> {}'.format(fromVersion, toVersion))
    
    good = True
    for key in ('repoOrder', 'repoDataDirs'):
        if key not in pipeline:
            caption(0, 'ERROR: no {} declared in the pipeline'.format(key))
            good = False
    if not good:
        return False
    for repo in pipeline['repoOrder'].strip().split():
        caption(2, 'Repo {}'.format(repo))
        if repo not in pipeline['repoDataDirs']:
            caption(0, 'Not specified which data directories I should copy over')
            continue
        dataDirs = pipeline['repoDataDirs'][repo].strip().split()
        for dataDir in dataDirs:
            fromDir = '{}/{}/{}/{}'.format(githubBase, repo, dataDir, fromVersion)
            toDir = '{}/{}/{}/{}'.format(githubBase, repo, dataDir, toVersion)
            caption(0, '\tCopy {}/{} ==> {}/{}'.format(dataDir, fromVersion, dataDir, toVersion))
            if os.path.exists(toDir):
                caption(0, '\t\tremoving existing {}/{}'.format(dataDir, toVersion))
                rmtree(toDir)
            else:
                caption(0, '\t\tno existing {}/{}'.format(dataDir, toVersion))
            if os.path.exists(fromDir):
                caption(0, '\t\tputting data in place from {}/{}'.format(dataDir, fromVersion))
                copytree(fromDir, toDir)
            else:
                caption(0, '\t\tNo data found in {}/{}'.format(dataDir, fromVersion))

def webPipeline(pipeline, version, force=False):
    caption(1, 'Aggregate MLQ for version {}'.format(version))
    good = True
    for key in (['repoOrder']):
        if key not in pipeline:
            caption(0, '\tERROR: no {} declared in the pipeline'.format(key))
            good = False
    if not good:
        return False

    repoOrder = pipeline['repoOrder'].strip().split()

    resultRepo = repoOrder[0]
    addedRepos = repoOrder[1:]

    resultRepoDir = '{}/{}'.format(githubBase, resultRepo)

    thisTempDir = '{}/_temp/{}'.format(resultRepoDir, version)
    tempShebanqDir = '{}/shebanq'.format(thisTempDir)
    shebanqDir = '{}/shebanq/{}'.format(resultRepoDir, version)

    dbName = '{}_xx'.format(resultRepo)

    mqlUFile = '{}/{}.mql'.format(tempShebanqDir, dbName)
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
            tfxDir = '{}/{}/tf/{}/.tf'.format(githubBase, repo, version)
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
        for (i, repo) in enumerate(repoOrder):
            locations.append('{}/{}/tf/{}'.format(githubBase, repo, version))

        TF = Fabric(locations=locations, modules=['']) 
        TF.exportMQL(dbName, tempShebanqDir)
    else:
        caption(0, '\tAlready up to date')

    caption(0, '\tbzipping {}'.format(mqlUFile))
    caption(0, '\tand delivering as {} ...'.format(mqlZFile))
    bzip(mqlUFile, mqlZFile)
    caption(0, '\tDone')

