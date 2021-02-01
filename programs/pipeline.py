import os
from subprocess import Popen, PIPE
from shutil import copy, copytree, rmtree
import nbformat
from nbconvert import PythonExporter
from tf.fabric import Fabric

from utils import bzip, caption

py = PythonExporter()

githubBase = os.path.expanduser("~/github/etcbc")
pipelineRepo = "pipeline"
utilsScript = "programs/utils.py"

programDir = "programs"
standardParams = "CORE_NAME VERSION".strip().split()


def runNb(repo, dirName, nb, force=False, **parameters):
    caption(3, "Run notebook [{}/{}] with parameters:".format(repo, nb))
    for (param, value) in sorted(parameters.items()):
        caption(0, "\t{:<20} = {}".format(param, value))

    location = "{}/{}/{}".format(githubBase, repo, dirName)
    nbFile = "{}/{}.ipynb".format(location, nb)
    pyFile = "{}/{}.py".format(location, nb)
    nbObj = nbformat.read(nbFile, 4)
    pyScript = py.from_notebook_node(nbObj)[0]
    with open(pyFile, "w") as s:
        s.write(pyScript)
    os.chdir(location)
    good = True
    with open(pyFile) as s:
        locals()["SCRIPT"] = True
        locals()["FORCE"] = force
        locals()["NAME"] = repo
        for (param, value) in parameters.items():
            locals()[param] = value

        try:
            exec(s.read(), locals())
        except SystemExit as inst:
            good = inst.args[0] == 0
        caption(0, "{} {}".format("SUCCESS" if good else "FAILURE", nb))

    caption(3, "[{}/{}]".format(repo, nb), good=good)
    return good


def checkRepo(repo, repoConfig, force=False, **parameters):
    good = True
    for item in repoConfig:
        task = item.get("task", None)
        if task is None:
            caption(0, "ERROR: missing task name in item {}".format(item))
            good = False

        for param in standardParams:
            if param not in parameters:
                caption(
                    0,
                    "ERROR: {} needs parameter {} which is not supplied".format(
                        task,
                        param,
                    ),
                )
                good = False
    return good


def runRepo(repo, repoConfig, force=False, **parameters):
    caption(2, "Make repo [{}]".format(repo))
    # copy the utils.py from the pipeline repo to the target repo
    copy(
        "{}/{}/{}".format(githubBase, pipelineRepo, utilsScript),
        "{}/{}/{}".format(githubBase, repo, utilsScript),
    )
    good = True
    for item in repoConfig:
        task = item["task"]
        omit = item.get("omit", set())
        paramValues = dict()
        for (param, values) in parameters.items():
            paramValues[param] = parameters[param]

        if "params" in item:
            paramValues.update(item["params"])
        version = paramValues.get("VERSION", "UNKNOWN")
        if version in omit:
            caption(3, "[{}/{}] skipped in version [{}]".format(repo, task, version))
            continue

        good = runNb(repo, programDir, task, force=force, **paramValues)
        if not good:
            break
    caption(2, "[{}]".format(repo), good=good)
    return good


def runRepos(repoOrder, repoConfig, force=False, **parameters):
    good = True
    for repo in repoOrder.strip().split():
        if repo not in repoConfig:
            caption(0, "ERROR: missing configuration for repo {}".format(repo))
            good = False
        if not checkRepo(repo, repoConfig[repo], **parameters):
            good = False
    if not good:
        return False

    for repo in repoOrder.strip().split():
        good = runRepo(repo, repoConfig[repo], force=force, **parameters)
        if not good:
            break
    return good


def runVersion(pipeline, version=None, force=False):
    caption(1, "Make version [{}]".format(version))

    good = True
    for key in ("defaults", "versions", "repoOrder", "repoConfig"):
        if key not in pipeline:
            if key == "defaults":
                if version is None:
                    caption(
                        0,
                        "ERROR: no {} version given and no known default section in pipeline",
                    )
                    good = False
            else:
                caption(0, "ERROR: no {} declared in the pipeline".format(key))
                good = False
        elif key == "defaults":
            if version is None:
                if "VERSION" not in pipeline["defaults"]:
                    caption(
                        0,
                        "ERROR: no version given and no default version specified in pipeline",
                    )
                    good = False
                else:
                    version = pipeline["defaults"]["VERSION"]
        elif key == "versions":
            if version not in pipeline["versions"]:
                if version is not None:
                    caption(
                        0, "ERROR: version {} not declared in pipeline".format(version)
                    )
                good = False
            else:
                versionInfo = pipeline["versions"][version]
    if not good:
        return False
    defaults = pipeline.get("defaults", {})
    # versions = pipeline["versions"]
    repoOrder = pipeline["repoOrder"]
    repoConfig = pipeline["repoConfig"]

    versionInfo = pipeline["versions"][version]
    paramValues = dict()
    for param in standardParams:
        if param == "VERSION":
            value = version
        else:
            value = versionInfo.get(param, defaults.get(param, None))
        if value is None:
            caption(0, "ERROR: no value or default value for {}".format(param))
            good = False
        else:
            paramValues[param] = value
    for (param, value) in defaults.items():
        if param in standardParams:
            continue
        paramValues[param] = value
    for (param, value) in versionInfo.items():
        if param in standardParams:
            continue
        paramValues[param] = value
    if not good:
        return False

    good = runRepos(repoOrder, repoConfig, force=force, **paramValues)
    caption(1, "[{}]".format(version), good=good)
    return good


def runPipeline(pipeline, versions=None, force=False):
    good = True
    chosenVersions = (
        [] if versions is None else [versions] if type(versions) is str else versions
    )
    for version in chosenVersions:
        thisGood = runVersion(pipeline, version=version, force=force)
        if not thisGood:
            good = False
    return good


def updateFeatures(toDir, toVersion):
    # the metadata in the feature files in toDir will change:
    # @version=fromVersion ====> @version=toVersion
    with os.scandir(toDir) as tfIt:
        for tfEntry in tfIt:
            if not tfEntry.is_file():
                continue
            featureFile = f"{toDir}/{tfEntry.name}"
            with open(featureFile) as fh:
                inLines = fh.readlines()
            with open(featureFile, "w") as fh:
                inMeta = True
                for line in inLines:
                    if inMeta:
                        if len(line) == 0 or line[0] != "@":
                            inMeta = False
                            fh.write(line)
                            continue
                        if line.startswith("@version="):
                            fh.write(f"@version={toVersion}\n")
                            continue
                        fh.write(line)
                        continue
                    fh.write(line)


def copyVersion(pipeline, fromVersion, toVersion):
    caption(1, "Copy version {} ==> {}".format(fromVersion, toVersion))

    good = True
    for key in ("repoOrder", "repoDataDirs"):
        if key not in pipeline:
            caption(0, "ERROR: no {} declared in the pipeline".format(key))
            good = False
    if not good:
        return False
    for repo in pipeline["repoOrder"].strip().split():
        caption(2, "Repo {}".format(repo))
        if repo not in pipeline["repoDataDirs"]:
            caption(0, "Not specified which data directories I should copy over")
            continue
        dataDirs = pipeline["repoDataDirs"][repo].strip().split()
        for dataDir in dataDirs:
            fromDir = "{}/{}/{}/{}".format(githubBase, repo, dataDir, fromVersion)
            toDir = "{}/{}/{}/{}".format(githubBase, repo, dataDir, toVersion)
            caption(
                0,
                "\tCopy {}/{} ==> {}/{}".format(
                    dataDir, fromVersion, dataDir, toVersion
                ),
            )
            if os.path.exists(toDir):
                caption(0, "\t\tremoving existing {}/{}".format(dataDir, toVersion))
                rmtree(toDir)
            else:
                caption(0, "\t\tno existing {}/{}".format(dataDir, toVersion))
            if os.path.exists(fromDir):
                caption(
                    0,
                    "\t\tputting data in place from {}/{}".format(dataDir, fromVersion),
                )
                copytree(fromDir, toDir)
                if dataDir == "tf":
                    caption(
                        0,
                        "\t\tadapting version in metadata of tf features to {}".format(
                            toVersion
                        ),
                    )
                    updateFeatures(toDir, toVersion)
            else:
                caption(0, "\t\tNo data found in {}/{}".format(dataDir, fromVersion))
        caption(2, "Repo {} done".format(repo))
    caption(1, "Version {} ==> {} copied".format(fromVersion, toVersion))


def run(cmd):
    p = Popen(
        [cmd], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True, universal_newlines=True
    )
    for line in p.stdout:
        caption(0, line)
    for line in p.stderr:
        caption(0, line)
    p.wait()
    return p.returncode == 0


def webPipeline(pipeline, versions=None, force=False, kinds={"mql", "mysql"}):
    good = True
    chosenVersions = (
        [] if versions is None else [versions] if type(versions) is str else versions
    )
    for version in chosenVersions:
        thisGood = webPipelineSingle(pipeline, version, force=force, kinds=kinds)
        if not thisGood:
            good = False
    return good


def webPipelineSingle(pipeline, version, force=False, kinds={"mql", "mysql"}):
    good = True

    if "mql" in kinds:
        caption(1, "Aggregate MQL for version {}".format(version))
        for key in ["repoOrder"]:
            if key not in pipeline:
                caption(0, "\tERROR: no {} declared in the pipeline".format(key))
                good = False
        if not good:
            return False

        repoOrder = pipeline["repoOrder"].strip().split()

        resultRepo = repoOrder[0]
        # addedRepos = repoOrder[1:]

        resultRepoDir = "{}/{}".format(githubBase, resultRepo)

        thisTempDir = "{}/_temp/{}".format(resultRepoDir, version)
        tempShebanqDir = "{}/shebanq".format(thisTempDir)
        shebanqDir = "{}/shebanq/{}".format(resultRepoDir, version)
        if not os.path.exists(shebanqDir):
            os.makedirs(shebanqDir)

        dbName = "shebanq_etcbc{}".format(version)

        mqlUFile = "{}/{}.mql".format(tempShebanqDir, dbName)
        mqlZFile = "{}/{}.mql.bz2".format(shebanqDir, dbName)

        xmU = os.path.exists(mqlUFile)
        # xmZ = os.path.exists(mqlZFile)

        uptodate = True

        referenceFile = mqlUFile if xmU else mqlZFile

        if not os.path.exists(referenceFile):
            uptodate = False
            caption(0, "\tWork to do because {} does not exist".format(referenceFile))
        else:
            tmR = os.path.getmtime(referenceFile)
            for (i, repo) in enumerate(repoOrder):
                tfxDir = "{}/{}/tf/{}/.tf".format(githubBase, repo, version)
                if not os.path.exists(tfxDir):
                    uptodate = False
                    caption(
                        0, "\tWork to do because the tf in {} is fresh".format(repo)
                    )
                    caption(0, "\t\t{}".format(tfxDir))
                    break
                if os.path.getmtime(tfxDir) > tmR:
                    uptodate = False
                    caption(
                        0,
                        "\tWork to do because the tf in {} is recently compiled".format(
                            repo
                        ),
                    )
                    caption(0, "\t\t{}".format(tfxDir))
                    break

        if uptodate and force:
            caption(0, "\tWork to do because you forced me to!")
            uptodate = False
        if not uptodate:
            caption(1, "Using TF to make an MQL export")
            locations = []
            for (i, repo) in enumerate(repoOrder):
                locations.append("{}/{}/tf/{}".format(githubBase, repo, version))

            TF = Fabric(locations=locations, modules=[""])
            TF.exportMQL(dbName, tempShebanqDir)
        else:
            caption(0, "\tAlready up to date")

        caption(0, "\tbzipping {}".format(mqlUFile))
        caption(0, "\tand delivering as {} ...".format(mqlZFile))
        bzip(mqlUFile, mqlZFile)
        caption(0, "\tDone")

    if "mysql" in kinds:
        caption(1, "Create Mysql passage db for version {}".format(version))
        runNb(pipelineRepo, programDir, "passageFromTf", force=force, VERSION=version)
        caption(0, "\tDone")

    return True


def importLocal(pipeline, versions=None, kinds={"mql", "mysql"}):
    good = True
    chosenVersions = (
        [] if versions is None else [versions] if type(versions) is str else versions
    )
    for version in chosenVersions:
        thisGood = importLocalSingle(pipeline, version, kinds=kinds)
        if not thisGood:
            good = False
    return good


def importLocalSingle(pipeline, version, kinds={"mql", "mysql"}):
    good = True

    if "mql" in kinds:
        repoOrder = pipeline["repoOrder"].strip().split()
        resultRepo = repoOrder[0]

        dbName = "shebanq_etcbc{}".format(version)
        dbDir = "{}/{}/shebanq/{}".format(githubBase, resultRepo, version)

        caption(1, "Import MQL db for version {} locally".format(version))
        dbDir = "{}/{}/_temp/{}/shebanq".format(githubBase, resultRepo, version)
        dbName = "shebanq_etcbc{}".format(version)
        if not run('mysql -u root -e "drop database if exists {};"'.format(dbName)):
            return False
        if not run("mql -n -b m -u root -e UTF8 < {}/{}.mql".format(dbDir, dbName)):
            return False

    if "mysql" in kinds:
        caption(1, "Import passage db for version {}".format(version))
        pdbName = "shebanq_passage{}".format(version)
        if not run("mysql -u root < {}/{}.sql".format(dbDir, pdbName)):
            return False

    return good


def copyServer(
    pipeline, user, server, remoteDir, versions=None, kinds={"mql", "mysql"}
):
    good = True
    chosenVersions = (
        [] if versions is None else [versions] if type(versions) is str else versions
    )
    for version in chosenVersions:
        thisGood = copyServerSingle(
            pipeline, user, server, remoteDir, version, kinds=kinds
        )
        if not thisGood:
            good = False
    return good


def copyServerSingle(
    pipeline, user, server, remoteDir, version, kinds={"mql", "mysql"}
):
    repoOrder = pipeline["repoOrder"].strip().split()
    resultRepo = repoOrder[0]

    dbDir = "{}/{}/shebanq/{}".format(githubBase, resultRepo, version)
    dbFile = "shebanq_etcbc{}.mql.bz2".format(version)
    pdbFile = "shebanq_passage{}.sql.gz".format(version)
    address = "{}@{}:{}".format(user, server, remoteDir)

    good = True
    for theFile in (dbFile, pdbFile):
        if theFile == dbFile and "mql" not in kinds:
            continue
        if theFile == pdbFile and "mysql" not in kinds:
            continue
        if theFile == dbFile:
            caption(1, "Sending MQL database for version {} to server".format(version))
        else:
            caption(
                1, "Sending passage database for version {} to server".format(version)
            )
        caption(0, "\t{}".format(theFile))
        caption(0, "\tscp {}/{} {}/{}".format(dbDir, theFile, address, theFile))
        if not run("scp {}/{} {}/{}".format(dbDir, theFile, address, theFile)):
            good = False
        caption(0, "\tdone")
    return good
