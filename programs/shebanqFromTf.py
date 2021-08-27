# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# ![pipeline](pictures/pictures.003.png)
#
# # SHEBANQ from Text-Fabric
#
# This notebook assembles data from relevant GitHub repositories of the ETCBC.
# It selects the data that is needed for the website
# [SHEBANQ](https://shebanq.ancient-data.org).
#
#
# ## Pipeline
# This is **pipe 2** of the pipeline from ETCBC data to the website SHEBANQ.
#
# A run of this pipe produces SHEBANQ data according to a chosen *version*.
# It should be run whenever there are new or updated data sources present that affect the output data.
# Since all input data is delivered in GitHub repositories, we have excellent machinery to
# work with versioning.
#
# Which directories the pipe should access for which version is specified in the configuration below.
#
# ### Core data
# The core data is what resides in
# the GitHub repo [BHSA](https://github.com/ETCBC/bhsa) in directory `tf`.
#
# This data will be converted by notebook `coreData` in its `programs` directory.
#
# The result of this action will be an updated Text-Fabric resource in its
# `tf` directory, under the chosen *version*.
#
# ### Additional data
#
# The pipe will try to load any text-fabric data features found in the `tf` subdirectories
# of the designated additional repos.
# It will descend one level deeper, according to the chosen *version*.
#
# ### Resulting data
# The resulting data will be delivered in the `shebanq` subdirectory of the core repo `bhsa`,
# and then under the chosen *version* subdirectory.
#
# The resulting data consists of three parts:
#
# * One big MQL file, containing the core data plus **all** additions: `bhsa-xx.mql`.
#   It will be bzipped.
# * An `sql` with database tables, containing everything SHEBANQ needs to construct its pages.
# * **not yet implemented**
#   A subdirectory `annotations`, containing bulk-uploadable annotation sets, that SHEBANQ can show in notes view,
#   between the clause atoms of the text.

# %load_ext autoreload
# %autoreload 2

from pipeline import webPipeline, importLocal, copyServer

# # Config

if "SCRIPT" not in locals():
    SCRIPT = False
    VERSIONS = ["2021"]
    KINDS = {"mql", "mysql"}

pipeline = dict(
    repoOrder="""
        bhsa
        phono
        valence
        parallels
    """,
)
user = "dirkr"
server = "clarin11.dans.knaw.nl"
remoteDir = "/home/dirkr/shebanq-install"

good = webPipeline(pipeline, versions=VERSIONS, force=True, kinds=KINDS)

# good = True
if good:
    good = importLocal(pipeline, versions=VERSIONS, kinds=KINDS)

# good = True
if good:
    good = copyServer(pipeline, user, server, remoteDir, versions=VERSIONS, kinds=KINDS)


