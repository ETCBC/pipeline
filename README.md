# Pipeline

![text-fabric](https://raw.github.com/ETCBC/text-fabric/master/docs/tf.png)

![ETCBC](etcbc.pnng)
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

# Purpose
The public data of the ETCBC is live data, in the sense that it is actively
developed at the ETCBC.
Mistakes are corrected, new insights are carried through, and the fruits of research are added as enrichments.

The ETCBC wants to expose its current data, for research purposes and to the public.
All public incarnations of its data at a given point in time should be in sync.

The refresh rate should be at least weekly, preferably more frequent.

# Method
The pipeline takes the data in the ETCBC repositories as input, 
recomputes the data transformations that it finds there,
commits the changes to Github,
aggregates the results,
and sends it to the SHEBANQ production server.

# Data

## Core
The main data source is the [bhsa](https://github.com/ETCBC/bhsa), the core data of the ETCBC.
It contains the text of the Hebrew Bible and systematic linguistic annotations (33 million and counting).

The ETCBC exposes this data as an
[MQL](https://emdros.org/mql.html)-dump, 
which is committed in bz2-compressed form to the *bhsa* repository, in its source directory.

The actual data is called version `c` (continuous).
There are also fixed versions, by year: `2017`, ...
And there are two legacy versions, `4` (2014), and `4b` (2015).

In the `programs` directory there are Jupyter notebooks that convert these mql files into
text-fabric data sources.

## Research
There is extra data in related repositories, such as
[valence](https://github.com/ETCBC/valence)
and
[parallels](https://github.com/ETCBC/parallels).

These repositories generate extra features next to the core data, that can be used
in text-fanric data processing.

They also produce manual annotation sets that can be bulk-imported into SHEBANQ.

Yet other repositories may generate new data from existing data, such as
[phono](https://github.com/ETCBC/parallels).

This is all delivered in text-fabric form, by Jupyter notebooks that use the core data.

## Website
The SHEBANQ website needs the data in two forms: mql for powering the 
[emdros](https://emdros.org) query engine, and *mysql* to power the display of text and annotated data.

# Operation
The pipeline is coded as a Jupyter notebook, but it can also be run as a script,
by converting it first to plain python by means of
[nbconvert](https://nbconvert.readthedocs.io/en/latest/).

The individual repositories also code their data processing in Jupyter notebooks.
When the pipeline runs, it find these notebooks, converts them, and runs them in a special mode.

The benefit of this approach is, that the data processing per repo can be developed
interactively in a notebook, without any pipeline concern.

Later, the bits that ere needed for the pipeline, can be brought under the scope of the special mode.

The pipeline ...

* Exports a full MQL version of the Hebrew Text database: `bhsa-c.mql.bz2`.
  This is in bzip2 format, a typical mql file is 500 MB, its bz2 version only 25MB.
  The `-c` indicates the continuous version; the same can be done for fixed versions, such as `4b` and `2017`, 
  so you'll find `bhsa-4b.mql.bz2` and `bhsa-2017.mql.bz2` here as well.
  From now one we describe the `c` version, but the pipeline for other versions is identical.
* Converts it to a text-fabric resource: `bhsa/core/c`.
  Under this directory you'll find the text-fabric *feature*`.tf` files, for every *feature* in the original MQL file.
* Enriches it with additional text-fabric data modules
  * `bhsa/stats`: statistical features,
  * `bhsa/phono`: phonetic transcription,
  * `bhsa/parallel`: parallel passages,
  * `bhsa/valence`: verbal valence.
  These repos contain methods to produce results from the core data, for several versions.
  These methods will check whether they can compute for the given version, and if so, they will (re)create their
  data in a version dependent directory.
* Commits the text-fabric data changes in all github repositories involved.
  From this point on, researchers have access to the updated data: they can pull it from github,
  start their text-fabric engines, and process it.
* Updates the website [shebanq](https://shebanq.ancient-data.org). It ...
  * Exports all text-fabric data (core plus modules) to one mql file: `x_bhsa-c.mql`.
  * Compiles all the text-fabric data into website friendly mysql databases.
  * Compiles annotation sets from the relevant text-fabric data modules, such as `parallels` and `valence`.
  * Transfers the new mql, mysql and annotations to the production server of SHEBANQ.
    Here the pipeline stops.
    The one last step to complete the data transfer must be done on the production server itself.

At the SHEBANQ production server ...
  * The `x_bhsa-c.mql` is imported into a live Emdros database.
    This is the database against which the shebanq queries are executed.
  * The mysql databases are imported into the live mysql database manager.
    These are the databases that display shebanq text and data views.
  * The generated annotation sets are imported in the the notes database (also a mysql database).
    From this database SHEBANQ fetches all manual annotations for display next to the text in notes view.
