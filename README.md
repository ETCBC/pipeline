# Pipeline

![text-fabric](https://raw.github.com/ETCBC/text-fabric/master/docs/tf.png)

**from**
![ETCBC](etcbc.png)
**to**
![text-fabric](https://raw.github.com/ETCBC/text-fabric/master/docs/tf.png)
**to**
![shebanq](shebanq.png)

# About
This is the connection between the Amsterdam Hebrew data of the 
[ETCBC](http://www.godgeleerdheid.vu.nl/en/research/institutes-and-centres/eep-talstra-centre-for-bible-and-computer/index.aspx)
and the website
[SHEBANQ](https://shebanq.ancient-data.org/sources).
of
[DANS](https://dans.knaw.nl/en/front-page?set_language=en).

This repo contains a pipeline in software by which the ETCBC can update its public data sources.
In the middle between the ETCBC and SHEBANQ there are open Github repositories that contain the data
in a compact, text-based format,
[text-fabric](https://github.com/ETCBC/text-fabric/wiki),
which is uniquely suited to frictionless data processing.

The pipeline has two main parts: from
[ETCBC to TF](https://github.com/ETCBC/pipeline/blob/master/programs/tfFromEtcbc.ipynb)
(text-fabric in Github repos), and from
[TF to SHEBANQ](https://github.com/ETCBC/pipeline/blob/master/programs/shebanqFromTF.ipynb).
Only the first part has been fully developed so far (as of **2017-09-29**),
the second part only partly.

# Purpose
The public data of the ETCBC is live data, in the sense that it is actively
developed at the ETCBC.
Mistakes are corrected, new insights are carried through,
and the fruits of research are added as enrichments.

The ETCBC wants to expose its current data, for research purposes and to the public.
All public incarnations of its data at a given point in time should be in sync.

The refresh rate should be at least weekly, preferably more frequent.

The pipeline produces *versions* of the whole spectrum of interconnected ETCBC data.
There will be fixed versions (`2017`, `2019`, ...) and a continuous version (`c`).

The name of the version is the most important parameter of the pipeline.

## Summary of the pipeline

### [ETCBC to TF](https://github.com/ETCBC/pipeline/blob/master/programs/tfFromEtcbc.ipynb)
* the ETCBC dumps the BHSA datasource on a weekly basis, and pushes it
  to the [BHSA](https://github.com/ETCBC/bhsa) repo;
  the data consists of:
  * a big MQL dump with most of the features,
  * several related data files in other formats, containing the lexicon, ketiv-qere data and paragraph numbers.
  The BHSA repo contains the notebooks to convert this all to a text-fabric data set, called **core**.
* the ETCBC maintains additional data modules
  * [valence](https://github.com/ETCBC/valence) (verbal valence),
  * [parallels](https://github.com/ETCBC/parallels) (parallel passages).
  * [phono](https://github.com/ETCBC/parallels) (phonetic transcription).
  These repos contain methods to produce new data from core data and to deliver
  that data as new text-fabric data modules.

### [TF to SHEBANQ](https://github.com/ETCBC/pipeline/blob/master/programs/shebanqFromTF.ipynb).
* aggregates all text-fabric data (core plus modules) to one big mql file;
* compiles all the text-fabric data into website friendly mysql databases;
* compiles annotation sets from the relevant text-fabric data modules, such as `parallels` and `valence`;
  
### At the SHEBANQ production server
* the aggregated MQL is imported into a live Emdros database,
  the database against which the shebanq queries are executed;
* the mysql databases are imported into the live mysql database system,
  which powers the display of shebanq text and data views;
* the generated annotation sets are imported in the the notes database (also a mysql database),
  from where SHEBANQ fetches all manual annotations for display next to the text in notes view.

# Operation
The pipeline is coded as a Jupyter notebook, but it can also be run as a script,
by converting it first to plain python by means of

The individual repositories also code their data processing in Jupyter notebooks.
When the pipeline runs, it find these notebooks, converts them, and runs them in a special mode.

The benefit of this approach is, that the data processing per repo can be developed
interactively in a notebook, without any pipeline concern.

Later, the bits that are needed for the pipeline, can be brought under the scope of the special mode.

## Script mode
Here is how the pipeline runs a notebook
* convert the script to python with
  [nbconvert](https://nbconvert.readthedocs.io/en/latest/).
* read the script as file and execute it as a python string, through the built-in
  [exec()](https://docs.python.org/3.6/library/functions.html#exec) function;
* supply arguments to the script by injecting them directly into 
  [locals()](https://docs.python.org/3.6/library/functions.html#locals)

We adopt the convention that the pipeline passes a boolean parameter `SCRIPT` with
value `True` to each notebook that it runs in this way.

Every notebook in the pipeline has to check (first thing) whether the variable `SCRIPT` is among
the `locals()`.

If not, the script knows that a user in interacting with it.
In that case, it is handy to set the remaining parameters that are relevant to the
pipeline to default values, e.g.

```python
if 'SCRIPT' not in locals():
    SCRIPT = False
    FORCE = True
    CORE_NAME = 'bhsa'
    VERSION = 'c'
    CORE_MODULE ='core' 
```

When run by the pipeline, `SCRIPT` *is* in `locals()`, and the variable assignments
in this cell do not take place.
Instead, the pipeline injects particular values for these variables.

Effectively, the notebook has turned into a generic function, to which you can pass parameters.

So the pipeline can run this notebook several times in a row for different versions,
without the need to change the notebook in any way.

When you compute interactively with the notebook, you may want to do things
that are not relevant to the pipeline.

That is easy. Just put your things under:

```python
if SCRIPT == False:
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
In interactive mode, after running the cell with `if SCRIPT in locals()`, 
we will have `SCRIPT == False`.
That means, from then on `SCRIPT is in locals()`.

So, if you, in your interactive session, want to change from `VERSION = 'c'` to
`VERSION = 'd'`, 
you'll discover that the staetement is not executed.

The best way to overcome this is to restart the kernel of the notebook.

If you do not want to loose your variables, just say

```python
if not SCRIPT: VERSION = 'd'
```

so that the pipeline operations do not get overriden by your specific choice.
