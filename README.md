# Pipeline

[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/ETCBC/pipeline/)](https://archive.softwareheritage.org/browse/origin/https://github.com/ETCBC/pipeline/)
[![DOI](https://zenodo.org/badge/104837219.svg)](https://doi.org/10.5281/zenodo.1153961)
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

[![etcbc](programs/images/etcbc.png)](http://www.etcbc.nl)
[![dans](programs/images/dans.png)](https://dans.knaw.nl/en)
[![tf](programs/images/tf-small.png)](https://annotation.github.io/text-fabric/tf)

![pipeline](programs/pictures/pictures.001.png)

### BHSA Family

* [bhsa](https://github.com/etcbc/bhsa) Core data and feature documentation
* [phono](https://github.com/etcbc/phono) Phonological representation of Hebrew words
* [parallels](https://github.com/etcbc/parallels) Links between similar verses
* [valence](https://github.com/etcbc/valence) Verbal valence for all occurrences
  of some verbs
* [trees](https://github.com/etcbc/trees) Tree structures for all sentences
* [bridging](https://github.com/etcbc/bridging) Open Scriptures morphology
  ported to the BHSA
* [pipeline](https://github.com/etcbc/pipeline) Generate the BHSA and SHEBANQ
  from internal ETCBC data files
* [shebanq](https://github.com/etcbc/shebanq) Engine of the
  [shebanq](https://shebanq.ancient-data.org) website

### Extended family

* [dss](https://github.com/etcbc/dss) Dead Sea Scrolls
* [extrabiblical](https://github.com/etcbc/extrabiblical)
  Extra-biblical writings from ETCBC-encoded texts
* [peshitta](https://github.com/etcbc/peshitta)
  Syriac translation of the Hebrew Bible
* [syrnt](https://github.com/etcbc/syrnt)
  Syriac translation of the New Testament

## About

This is the connection between the Amsterdam Hebrew data of the 
[ETCBC](http://etcbc.nl)
and the website
[SHEBANQ](https://github.com/ETCBC/shebanq/wiki/Sources)
of
[DANS](https://dans.knaw.nl/en/front-page?set_language=en).

## Portable ETCBC data
This pipeline delivers, among other good things, a file `shebanq_etcbc2021.mql.bz2`
which contains all ETCBC data and research additions to it.
The form is MQL, compressed, and the size is less than 30 MB.
Where ever you have [Emdros](https://emdros.org) installed,
you can query this data.

## Two pipes
This repo contains a pipeline in software by which the ETCBC can update
its public data sources.
The pipeline has two main pipes:
* [ETCBC to TF](https://GitHub.com/ETCBC/pipeline/blob/master/programs/tfFromEtcbc.ipynb)
* [TF to SHEBANQ](https://GitHub.com/ETCBC/pipeline/blob/master/programs/shebanqFromTf.ipynb).

Between the two pipes there is a set of open GitHub repositories that contain the data
in a compact, text-based format,
[text-fabric](https://github.com/annotation/text-fabric),
which is uniquely suited to frictionless data processing.

## Purpose
The public data of the ETCBC is live data, in the sense that it is actively
developed at the ETCBC.
Mistakes are corrected, new insights are carried through,
and the fruits of research are added as enrichments.
This leads to new versions of the data, once in every few years.
Whenever a new version has to be produced, this pipeline manages all the details,
and helps to prevent omissions.

## Buffer function
Between versions, many things can happen to the contents and organization
of the data features.
This pipeline is a useful tool to deal with those challenges.

## Versioning
The pipeline produces *versions* of the whole spectrum of interconnected ETCBC data.
These versions (`2017`, `2021`, ...), once published, will not change anymore
in essential ways.
It might be the case that certain aspects of the feature organization will be changed,
but these changes do not reflect data updates by the ETCBC.
Think of conversion errors by an earlier run of the pipeline.
Sometimes we add data redundantly in order to make certain queries easier.

## Author

[Dirk Roorda](https://github.com/dirkroorda)

With useful feedback of Cody Kingham and Christiaan Erwich.

## Summary of the pipeline

### [ETCBC to TF](https://GitHub.com/ETCBC/pipeline/blob/master/programs/tfFromEtcbc.ipynb)
The ETCBC dumps the data files of a new data version
in the [BHSA](https://GitHub.com/ETCBC/bhsa) repo,
in the subfolder `source/`*version*.

The data consists of:
* a big MQL dump with most of the features,
* several related data files in other formats, containing
  * the lexicon,
  * ketiv-qere data and
  * paragraph numbers.

From there this pipeline takes over.
The BHSA repo contains the notebooks to convert this all to a text-fabric data set,
called **core**.

But this is not all.

The ETCBC maintains additional GitHub repositories.
* [valence](https://GitHub.com/ETCBC/valence) (verbal valence),
* [parallels](https://GitHub.com/ETCBC/parallels) (parallel passages),
* [phono](https://GitHub.com/ETCBC/parallels) (phonological transcription).
* [trees](https://GitHub.com/ETCBC/trees) (tree structures).
* [bridging](https://GitHub.com/ETCBC/trees) (Open Scriptures morphology).
These repos contain methods to produce new data from core data (and third party data)
and to deliver that as new text-fabric data modules.

When the pipeline runs, it finds those methods and executes them.

### [TF to SHEBANQ](https://GitHub.com/ETCBC/pipeline/blob/master/programs/shebanqFromTF.ipynb).
This part of the pipeline:
* aggregates all text-fabric data (core plus modules) to one big MQL file,
* compiles all the text-fabric data into website friendly MYSQL databases,
* compiles annotation sets from the relevant text-fabric data modules,
  such as `parallels` and `valence`.
  
## Post pipeline steps

### After ETCBC to TF
All repos involved should commit and push to GitHub,
in order for the outside world to see the changes.
For each `repo` (currently: `bhsa`, `phono`, `parallels`, `valence`, `trees`, `bridging`)

```sh
cd ~/GitHub/etcbc/repo
git add --all .
git commit "pipeline has run"
git push origin master
```

**Caveat**
It is wise to perform a `git pull origin master` after the commit,
in case other users have committed changes to GitHub.


### After TF to SHEBANQ
The following steps should be done at the production server:
* the aggregated MQL is imported into a live Emdros database,
  the database against which the SHEBANQ queries are executed;
* the MYSQL databases are imported into the live MYSQL database system,
  which powers the display of SHEBANQ text and data views;
* the generated annotation sets are imported in the notes database
  (also a MYSQL database),
  from where SHEBANQ fetches all manual annotations for display
  next to the text in notes view.
  Existing incarnations of these note sets should be deleted first.

## Operation
The pipeline is coded as a Jupyter notebook, but it can also be run as a script,
by converting it first to plain Python.

The individual repositories also code their data processing in Jupyter notebooks.
When the pipeline runs, it find these notebooks, converts them,
and runs them in a special mode.

The benefit of this approach is, that the data processing per repo can be developed
interactively in a notebook, without any pipeline concern.

Later, the bits that are needed for the pipeline,
can be brought under the scope of the special mode.

If the pipeline runs these notebooks, and they produce errors,
you can go to the faulty notebook, run it interactively,
diagnose the misbehaviour and fix it.

### Script mode
Here is how the pipeline runs a notebook
* convert the notebook to python with
  [nbconvert](https://nbconvert.readthedocs.io/en/latest/);
* read the script as file and execute it as a python string, through the built-in
  [exec()](https://docs.python.org/3.6/library/functions.html#exec) function;
* supply arguments to the script by injecting them directly into 
  [locals()](https://docs.python.org/3.6/library/functions.html#locals).

We adopt the convention that the pipeline passes a boolean parameter `SCRIPT` with
value `True` to each notebook that it runs in this way.

Every notebook in the pipeline has to check (first thing)
whether the variable `SCRIPT` is among the `locals()`.

If not, the script knows that a user in interacting with it.
In that case, it is handy to set the remaining parameters that are relevant to the
pipeline to the values that you want for your interactive sessions, e.g.

```python
if 'SCRIPT' not in locals():
    SCRIPT = False
    FORCE = True
    CORE_NAME = 'bhsa'
    VERSION = 'c'
    CORE_MODULE ='core' 
```

These settings will not be seen by the pipeline!
When run by the pipeline, `'SCRIPT' in locals()` is true,
and the variable assignments in this cell do not take place.
Instead, the pipeline injects particular values for these variables.

Effectively, the notebook has turned into a generic function,
to which you can pass parameters.

So the pipeline can run a notebook several times in a row for different versions,
without the need to change the notebook in any way.

When you compute interactively with the notebook, you may want to do things
that are not relevant to the pipeline.

That is easy. Just put your things under:

```python
if not SCRIPT:
    # my things ...
```

or even better, define the function

```
def stop(good=False):
    if SCRIPT: sys.exit(0 if good else 1)
```

and take care that in script mode this function is called at the end of the operations
relevant for the pipeline:

```python
success = lastOperation(data)
stop(good=succes)
```

Whatever you do in cells after this statement, it will not be reached by the pipeline.

**Caveat**
In interactive mode, after running the cell with `if 'SCRIPT' not in locals()`, 
we will have `SCRIPT == False`.
That means, from then on the variable `SCRIPT` exists and `'SCRIPT' in locals()` is true.

So, if you, in your interactive session, want to change from `VERSION = 'c'` to
`VERSION = '2017'`, 
you'll discover that this statement is not executed.

The best way to overcome this is to restart the kernel of the notebook.

If you do not want to loose your variables, just say

```python
if not SCRIPT: VERSION = '2017'
```

so that the pipeline operations do not get overridden by your specific choice.
