#!/usr/bin/env python
# coding: utf-8

# <img align="right" src="images/dans-small.png"/>
# <img align="right" src="images/tf-small.png"/>
# <img align="right" src="images/etcbc.png"/>
# 
# 
# #  Passage from Tf
# 
# This creates passage databases for SHEBANQ.
# These are MYSQL tables, populated with data that SHEBANQ needs to show its pages.
# Most data comes from the BHSA repository, but we also need a few features from PHONO.
# 
# We do not need material from PARALLELS and VALENCE, because they deliver their results for SHEBANQ
# in the form of sets of notes.
# 
# ## Discussion
# 

# In[1]:


import os,sys,re,collections
import utils
from tf.fabric import Fabric


# # Pipeline
# See [operation](https://github.com/ETCBC/pipeline/blob/master/README.md#operation) 
# for how to run this script in the pipeline.

# In[2]:


if 'SCRIPT' not in locals():
    SCRIPT = False
    FORCE = True
    VERSION= '4'

def stop(good=False):
    if SCRIPT: sys.exit(0 if good else 1)


# # Setting up the context: source file and target directories
# 
# The conversion is executed in an environment of directories, so that sources, temp files and
# results are in convenient places and do not have to be shifted around.

# In[3]:


CORE_NAME = 'bhsa'
PHONO_NAME = 'phono'

repoBase = os.path.expanduser('~/github/etcbc')
thisRepo = '{}/{}'.format(repoBase, CORE_NAME)
phonoRepo = '{}/{}'.format(repoBase, PHONO_NAME)
tfDir = 'tf/{}'.format(VERSION)

thisTemp = '{}/_temp/{}'.format(thisRepo, VERSION)
thisTempMysql = '{}/shebanq'.format(thisTemp)
thisMysql = '{}/shebanq/{}'.format(thisRepo, VERSION)

passageDb = 'shebanq_passage{}'.format(VERSION)

mysqlZFile = '{}/{}.sql.gz'.format(thisMysql, passageDb)
mysqlFile = '{}/{}.sql'.format(thisTempMysql, passageDb)


# # Test
# 
# Check whether this conversion is needed in the first place.
# Only when run as a script.

# In[4]:


if SCRIPT:
    (good, work) = utils.mustRun(None, mysqlZFile, force=FORCE)
    if not good: stop(good=False)
    if not work: stop(good=True)


# In[5]:


for path in (thisMysql, thisTempMysql):
    if not os.path.exists(path):
        os.makedirs(path)


# # Collect
# 
# We collect the data from the TF repos.

# In[6]:


utils.caption(4, 'Loading relevant features')

if VERSION in {'4', '4b'}:
    QERE = 'g_qere_utf8'
    QERE_TRAILER = 'qtrailer_utf8'
    ENTRY = 'g_entry'
    ENTRY_HEB = 'g_entry_heb' 
    PHONO_TRAILER = 'phono_sep'
    LANGUAGE = 'language'
else:
    QERE = 'qere_utf8'
    QERE_TRAILER= 'qere_trailer_utf8'
    ENTRY = 'voc_lex'
    ENTRY_HEB = 'voc_lex_utf8'
    PHONO_TRAILER = 'phono_trailer'
    LANGUAGE = 'languageISO'

    
TF = Fabric(locations=[thisRepo, phonoRepo], modules=[tfDir])
api = TF.load(f'''
        g_cons g_cons_utf8 g_word g_word_utf8 trailer_utf8
        {QERE} {QERE_TRAILER}
        {LANGUAGE} lex g_lex lex_utf8 sp pdp ls
        {ENTRY} {ENTRY_HEB}
        vt vs gn nu ps st
        nme pfm prs uvf vbe vbs
        gloss nametype root ls
        pargr
        phono {PHONO_TRAILER}
        function typ rela txt det
        code tab
        number
        freq_lex freq_occ
        rank_lex rank_occ
        book chapter verse
''')
api.makeAvailableIn(globals())

hasLex = 'lex' in set(F.otype.all)


# # Data model
# 
# The data model of the browsing database as as follows:
# 
# There are tables ``book``, ``chapter``, ``verse``, ``word_verse``, ``lexicon``, ``clause_atom``.
# 
# The tables ``book``, ``chapter``, ``verse``, ``clause_atom`` contain fields ``first_m``, ``last_m``, 
# denoting the first and last monad number of that book, chapter, verse, clause_atom.
# 
# A ``book``-record contains an identifier and the name of the book.
# 
# A ``chapter``-record contains an identifier, the number of the chapter, and a foreign key to the record in the ``book`` table to which the chapter belongs.
# 
# A ``verse``-record contains an identifier, the number of the verse, and a foreign key to the record in the ``chapter`` table to which the verse belongs. More over, it contains the text of the whole verse in two formats:
# 
# In field ``text``: the plain Unicode text string of the complete verse.
# 
# In field ``xml``: a sequence of ``<w>`` elements, one for each word in the verse, containing the plain Unicode text string of that word as element content.
# The monad number of that word is stored in an attribute value. 
# The monad number is a globally unique sequence number of a word occurrence in the Hebrew Bible, going from 1 to precisely 426,555.
# There is also a lexical identifier stored in an attribute value.
# The lexical identifier points to the lexical entry that corresponds with the word.
# 
#     <w m="2" l="3">רֵאשִׁ֖ית </w>
# 
# As you see, the material between a word and the next word is appended to the first word. So, when you concatenate words, whitespace or other separators are needed.
# 
# A ``word_verse``-record links a word to a verse. 
# The monad number is in field ``anchor``, which is an integer, 
# and the verse is specified in the field ``verse_id`` as foreign key.
# The field ``lexicon_id`` is a foreign key into the ``lexicon`` table.
# 
# There is also a ``word`` table, meant to store all the information to generate a rich representation of the Hebrew text,
# its syntactic structure, and some linguistic properties.
# See that notebook for a description and an example of the rich Hebrew text representation.
# 
# The rich data is added per word, but the data has a dependency on the verses the words are contained in.
# In general, information about sentences, clauses and phrases will be displayed on the first words of those objects,
# but if the object started in a previous verse, this information is repeated on the first word of that object in the
# current verse.
# This insures that the display of a verse is always self-contained.
# 
# The ``word`` table has no field ``id``, its primary key is the field called ``word_number``. 
# This fields contains the same monad number as is used in the field ``anchor`` of the table ``word_verse``.
# 
# A ``clause_atom`` record contains an identifier, and the book to which it belongs, and its sequence number within 
# that book.
# In SHEBANQ, manual annotations are linked to the clause atom, so we need this information to easily fetch comments to
# passages and to compose charts and CSV files.
# 
# ## Lexicon
# 
# A ``lexicon`` record contains the various lexical fields, such as identifiers, entry representations,
# additional lexical properties, and a gloss.
# 
# We make sure that we translate lexical feature values into values used for the BHSA.
# We need the following information per entry:
# 
# * **id** a fresh id (see below), to be used in applications, unique over **entryid** and **lan**
# * **lan** the language of the entry, in ISO 639-3 abbreviation
# * **entryid** the string used as entry in the lexicon and as value of the ``lex`` feature in the text
# * **g_entryid** the Hebrew un-transliteration of entryid, with the disambiguation marks unchanged, corresponds to the ``lex_utf8`` feature
# * **entry** the unpointed transliteration (= **entryid** without disambiguation marks)
# * **entry_heb** the unpointed hebrew representation, obtained by un-transliterating **entry**
# * **g_entry** the pointed transliteration, without disambiguation marks, obtained from ``vc``
# * **g_entry_heb** the pointed hebrew representation, obtained by un-transliterating **g_entry**
# * **root** the root, obtained from ``rt``
# * **pos** the part of speech, obtained from ``sp``
# * **nametype** the type of named entity, obtained from ``sm``
# * **subpos** subtype of part of speech, obtained from ``ls`` (aka *lexical set*)
# * **gloss** the gloss from ``gl``
# 
# We construct the **id** from the ``lex`` feature as follows:
# 
# * allocate a varchar(32)
# * the > is an alef, we translate it to A
# * the < is an ayin, we translate it to O
# * the / denotes a noun, we translate it to n
# * the \[ denotes a verb, we translate it to v
# * the = is for disambiguation, we translate it to i
# * we prepend a language identifier, 1 for Hebrew, 2 for Aramaic.
# 
# This is sound, see the scheck in the extradata/lexicon notebook

# # Field transformation
# 
# The lexical fields require a bit of attention.
# The specification in ``lexFields`` below specifies the lexicon fields in the intended order.
# It contains instructions how to construct the field values from the lexical information obtained from the lexicon files.
# 
#     (source, method, name, transformation table, data type, data size, data options, params)
# 
# ## source 
# May contain one of the following:
# 
# * the name of a lexical feature as shown in the lexicon files, such as ``sp``, ``vc``.
# * None. 
#   In this case, **method** is a code that triggers special actions, such as getting an id or something that is available to the   program that fills the lexicon table
# * the name of an other field as shown in the **name** part of the specification. 
#   In this case, **method** is a function, defined else where, that takes the value of that other field as argument. 
#   The function is typically a transliteration, or a stripping action.
# 
# ## method
# May contain one of the following:
# 
# * a code (string), indicating:
#     * ``lex``: take the value of a feature (indicated in **source**) for this entry from the lexicon file
#     * ``entry``: take the value of the entry itself as found in the lexicon file
#     * ``id``: take the id for this entry as generated by the program
#     * ``lan``: take the language of this entry
# * a function taking one argument
#     * *strip_id*: strip the non-lexeme characters at the end of the entry (the ``/ [ =`` characters)
#     * *toHeb*: transform the transliteration into real Unicode Hebrew
#     * feature lookup functions such as ``F.lex.v``
# 
# ## name
# The name of the field in the to be constructed table ``lexicon`` in the database ``passage``.
# 
# ## data type
# The sql data type, such as ``int`` or ``varchar``, without the size and options.
# 
# ## data size
# The sql data size, which shows up between ``()`` after the data type
# 
# ## data options
# Any remaining type specification, such as `` character set utf8``.
# 
# ## params
# Params consists currently of 1 boolean, indicating whether the field is defined on all words of the object, or only on its first word.

# # Index of lexicon

# In[7]:


lexEntries = {}

for w in F.otype.s('word'):
    lan = Fs(LANGUAGE).v(w)
    lex = F.lex.v(w)
    lex_utf8 = F.lex_utf8.v(w)
    if lan in lexEntries and lex in lexEntries[lan]: continue

    lexId = '{}{}'.format(
        '1' if lan == 'hbo' else '2',
        lex.
            replace('>','A').
            replace('<','O').
            replace('[','v').
            replace('/','n').
            replace('=','i'),
    )


    lex0 = lex.rstrip('[/=]')
    lexDis = '' if lex0 == lex else lex[len(lex0)-len(lex):]
    
    refNode = L.u(w, otype='lex')[0]
    
    vocLexNode = w if ENTRY == 'g_entry' else refNode
    
    voc_lex = Fs(ENTRY).v(vocLexNode)
    voc_lex_utf8 = Fs(ENTRY_HEB).v(vocLexNode)
    
    root = F.root.v(refNode)
    sp = F.sp.v(refNode)
    nametype = F.nametype.v(refNode)
    ls = F.ls.v(refNode)
    gloss = F.gloss.v(refNode)
    
    lexEntries.setdefault(lan, {})[lex] = dict(
        id=lexId,
        lan=lan,
        entryid=lex,
        entry=lex0,
        entry_heb=lex_utf8,
        entryid_heb=lex_utf8+lexDis,
        g_entry=voc_lex,
        g_entry_heb=voc_lex_utf8,
        root=root if root != None else '',
        pos=sp if sp != None else '',
        nametype=nametype if nametype != None else '',
        subpos=ls if ls != None else '',
        gloss=gloss if gloss != None else '',

    )

for lan in sorted(lexEntries):
    utils.caption(0, 'Lexicon {} has {:>5} entries'.format(lan, len(lexEntries[lan])))


# # Index of ketiv/qere
# 
# We make a list of the ketiv-qere items.
# It will be used by the *heb* and the *ktv* functions.
# 
# *heb()* provides the surface text of a word.
# When the qere is different from the ketiv, the vocalized qere is chosen.
# It is the value of ``g_word_utf8`` except when a qere is present, 
# in which case it is ``g_qere_utf8``, preceded by a masora circle.
# This is the sign for the user to use data view to inspect the *ketiv*.
# 
# *ktv()* provides the surface text of a word, in case the ketiv is different from the qere.
# It is the value of ``g_word_utf8`` precisely when a qere is present, 
# otherwise it is empty.

# In[8]:


qeres = {}
masora = '֯'
utils.caption(0, 'Building qere index')
for w in F.otype.s('word'):
    q = Fs(QERE).v(w)
    if q != None:
        qeres[w] = (masora+q, Fs(QERE_TRAILER).v(w))
utils.caption(0, 'Found {} qeres'.format(len(qeres)))


# # Index of paragraphs
# 
# We make a list of paragraph numbers of clause_atoms. It will be used by the *para* function.

# In[9]:


paras = {}
utils.caption(0, 'Building para index')
for c in F.otype.s('clause_atom'):
    par = F.pargr.v(c)
    paras[c] = par
utils.caption(0, 'Found para information for {} clause_atoms'.format(len(paras)))


# ## Field types

# In[10]:


def strip_id(entryid):
    return entryid.rstrip('/[=')

def toHeb(translit):
    return Transcription.toHebrew(Transcription.suffix_and_finales(translit)[0])

def ide(n): return n

def heb(n):
    if n in qeres:
        (trsep, wrdrep) = qeres[n]
    else:
        trsep = F.trailer_utf8.v(n)
        wrdrep = F.g_word_utf8.v(n)
    if trsep.endswith('ס') or trsep.endswith('פ'): trsep += ' '
    return wrdrep + trsep

def ktv(n):
    if n in qeres:
        trsep = F.trailer_utf8.v(n)
        if trsep.endswith('ס') or trsep.endswith('פ'): trsep += ' '
        return F.g_word_utf8.v(n) + trsep    
    return ''

def para(n): return paras.get(n, '')

def lang(n):
    return Fs(LANGUAGE).v(n)

def df(f):
    def g(n): 
        val = f(n)
#        if val == None or val == "None" or val == "none" or val == "NA" or val == "N/A" or val == "n/a":
        if val == None:
            return '#null'
        return val
    return g

def dfl(f):
    def g(n): 
        val = f(L.u(n, otype='lex')[0])
#        if val == None or val == "None" or val == "none" or val == "NA" or val == "N/A" or val == "n/a":
        if val == None:
            return 'NA'
        return val
    return g

lexFields = (
    ('id', 'varchar', 32, ' primary key'),
    ('lan', 'char', 4, ''),
    ('entryid', 'varchar', 32, ''),
    ('entry', 'varchar', 32, ''),
    ('entry_heb', 'varchar', 32, ' character set utf8'),
    ('entryid_heb', 'varchar', 32, ' character set utf8'),
    ('g_entry', 'varchar', 32, ''),
    ('g_entry_heb', 'varchar', 32, ' character set utf8'),
    ('root', 'varchar', 32, ''),
    ('pos', 'varchar', 8, ''),
    ('nametype', 'varchar', 16, ''),
    ('subpos', 'varchar', 8, ''),
    ('gloss', 'varchar', 32, ' character set utf8'),
)
wordFields = (
    (ide, 'number', 'word', 'int', 4, ' primary key', False),
    (heb, 'heb', 'word', 'varchar', 32, ' character set utf8', False),
    (ktv, 'ktv', 'word', 'varchar', 32, ' character set utf8', False),
    (dfl(Fs(ENTRY_HEB).v), 'vlex', 'word', 'varchar', 32, ' character set utf8', False),
    (F.lex_utf8.v, 'clex', 'word', 'varchar', 32, ' character set utf8', False),
    (F.g_word.v, 'tran', 'word', 'varchar', 32, ' character set utf8', False),
    (F.phono.v, 'phono', 'word', 'varchar', 32, ' character set utf8', False),
    (Fs(PHONO_TRAILER).v, 'phono_sep', 'word', 'varchar', 8, ' character set utf8', False),
    (F.lex.v, 'lex', 'word', 'varchar', 32, ' character set utf8', False),
    (F.g_lex.v, 'glex', 'word', 'varchar', 32, ' character set utf8', False),
    (dfl(F.gloss.v), 'gloss', 'word', 'varchar', 32, ' character set utf8', False),
    (lang, 'lang', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.sp.v), 'pos', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.pdp.v), 'pdp', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.ls.v), 'subpos', 'word', 'varchar', 8, ' character set utf8', False),
    (dfl(F.nametype.v), 'nmtp', 'word', 'varchar', 32, ' character set utf8', False),
    (df(F.vt.v), 'tense', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.vs.v), 'stem', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.gn.v), 'gender', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.nu.v), 'gnumber', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.ps.v), 'person', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.st.v), 'state', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.nme.v), 'nme', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.pfm.v), 'pfm', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.prs.v), 'prs', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.uvf.v), 'uvf', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.vbe.v), 'vbe', 'word', 'varchar', 8, ' character set utf8', False),
    (df(F.vbs.v), 'vbs', 'word', 'varchar', 8, ' character set utf8', False),
    (F.freq_lex.v, 'freq_lex', 'word', 'int', 4, '', False),
    (F.freq_occ.v, 'freq_occ', 'word', 'int', 4, '', False),
    (F.rank_lex.v, 'rank_lex', 'word', 'int', 4, '', False),
    (F.rank_occ.v, 'rank_occ', 'word', 'int', 4, '', False),
    (None, 'border', 'subphrase', 'varchar', 16, ' character set utf8', False),
    ('id', 'number', 'subphrase', 'varchar', 32, ' character set utf8', False),
    (df(F.rela.v), 'rela', 'subphrase', 'varchar', 8, ' character set utf8', True),
    (None, 'border', 'phrase', 'varchar', 8, ' character set utf8', False),
    (F.number.v, 'number', 'phrase_atom', 'int', 4, '', False),
    (df(F.rela.v), 'rela', 'phrase_atom', 'varchar', 8, ' character set utf8', True),
    (F.number.v, 'number', 'phrase', 'int', 4, '', False),
    (df(F.function.v), 'function', 'phrase', 'varchar', 8, ' character set utf8', True),
    (df(F.rela.v), 'rela', 'phrase', 'varchar', 8, ' character set utf8', True),
    (df(F.typ.v), 'typ', 'phrase', 'varchar', 8, ' character set utf8', True),
    (df(F.det.v), 'det', 'phrase', 'varchar', 8, ' character set utf8', True),
    (None, 'border', 'clause', 'varchar', 8, ' character set utf8', False),
    (F.number.v, 'number', 'clause_atom', 'int', 4, '', False),
    (df(F.code.v), 'code', 'clause_atom', 'int', 4, '', True),
    (df(F.tab.v), 'tab', 'clause_atom', 'int', 4, '', False),
    (para, 'pargr', 'clause_atom', 'varchar', 64, ' character set utf8', True),
    (F.number.v, 'number', 'clause', 'int', 4, '', False),
    (df(F.rela.v), 'rela', 'clause', 'varchar', 8, ' character set utf8', True),
    (df(F.typ.v), 'typ', 'clause', 'varchar', 8, ' character set utf8', True),
    (df(F.txt.v), 'txt', 'clause', 'varchar', 8, ' character set utf8', False),
    (None, 'border', 'sentence', 'varchar', 8, ' character set utf8', False),
    (F.number.v, 'number', 'sentence_atom', 'int', 4, '', False),
    (F.number.v, 'number', 'sentence', 'int', 4, '', False),
)
firstOnly = dict(('{}_{}'.format(f[2], f[1]), f[6]) for f in wordFields)


# # Sanity
# The texts and XML representations of verses are stored in ``varchar`` fields.
# We have to make sure that the values fit within the declared sizes of these fields.
# The code measures the maximum lengths of these fields, and it turns out that the text is maximally 434 chars and the XML 2186 chars.

# In[11]:


fieldLimits = {
    'book': {
        'name': 32,
    },
    'verse': {
        'text': 1024,
        'xml': 4096,
    },
    'clause_atom': {
        'text': 512,
    },
    'lexicon': {},
}
for f in lexFields:
    if f[1].endswith('char'):
        fieldLimits['lexicon'][f[0]] = f[2]

config = {
    'db': 'shebanq_passage'+VERSION,
}
for tb in fieldLimits:
    for fl in fieldLimits[tb]: config['{}_{}'.format(tb, fl)] = fieldLimits[tb][fl]

textCreateSql = '''
set character_set_client = 'utf8';
set character_set_connection = 'utf8';

drop database if exists {db};

create database {db} character set utf8;

use {db};

create table book(
    id      int(4) primary key,
    first_m int(4),
    last_m int(4),
    name varchar({book_name}),
    index(name)
);

create table chapter(
    id int(4) primary key,
    first_m int(4),
    last_m int(4),
    book_id int(4),
    chapter_num int(4),
    foreign key (book_id) references book(id),
    index(chapter_num)
);

create table verse(
    id int(4) primary key,
    first_m int(4),
    last_m int(4),
    chapter_id int(4),
    verse_num int(4),
    text varchar({verse_text}) character set utf8,
    xml varchar({verse_xml}) character set utf8,
    foreign key (chapter_id) references chapter(id)
);

create table clause_atom(
    id int(4) primary key,
    first_m int(4),
    last_m int(4),
    ca_num int(4),    
    book_id int(4),
    text varchar({clause_atom_text}) character set utf8,
    foreign key (book_id) references book(id),
    index(ca_num)
);

create table word(
    {{wordfields}}
);

create table lexicon(
    {{lexfields}}    
) collate utf8_bin;

create table word_verse(
    anchor int(4) unique,
    verse_id int(4),
    lexicon_id varchar(32),
    foreign key (anchor) references word(word_number),
    foreign key (verse_id) references verse(id),
    foreign key (lexicon_id) references lexicon(id)
) collate utf8_bin;

'''.format(**config).format(
        lexfields = ',\n    '.join('{} {}({}){}'.format(
            f[0], f[1], f[2], f[3],
        ) for f in lexFields),
        wordfields = ', \n    '.join('{}_{} {}({}){}'.format(
            f[2], f[1], f[3], f[4], f[5],
    ) for f in wordFields),
)
if not SCRIPT:
    print(textCreateSql)


# # Table filling
# 
# We compose all the records for all the tables.
# 
# We also generate a file that can act as the basis of an extra annotation file with lexical information.

# In[13]:


utils.caption(4, 'Fill the tables ... ')
curId = {
    'book': -1,
    'chapter': - 1,
    'verse': -1,
    'clause_atom': -1
}

def sEsc(sql): return sql.replace("'", "''").replace('\\','\\\\').replace('\n','\\n')

curVerseNode = None
curVerseInfo = []
curVerseFirstSlot = None
curVerseLastSlot = None
curLexValues = {}

lexIndex = {}
lexNotFound = collections.defaultdict(lambda: collections.Counter())
tables = collections.defaultdict(lambda: [])
fieldSizes = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))

Fotypev = F.otype.v
Ftextv = F.g_word_utf8.v
Foccv = F.g_cons.v
Flexv = F.lex.v
Flanguagev = Fs(LANGUAGE).v
Ftrailerv = F.trailer_utf8.v
Fnumberv = F.number.v

def computeFields(entryData):
    eId = entryData['id']
    lan = entryData['lan']
    entryid = entryData['entryid']
    lexIndex[(lan, entryid)] = eId
    result = []
    for (fName, fType, fSize, fRest) in lexFields:
        val = entryData[fName]
        val = 'null' if val == None else             val if fType == 'int' else             "'{}'".format(sEsc(val))
        if fName in fieldLimits['lexicon']:
            fieldSizes['lexicon'][fName] = max(len(val)-2, fieldSizes['lexicon'][fName])
        result.append(val)        
    return result

for lan in sorted(lexEntries):
    for (entry, entryData) in sorted(lexEntries[lan].items()):
        format_str = '({})'.format(','.join('{}' for f in lexFields))
        entryInfo = computeFields(entryData)
        tables['lexicon'].append(format_str.format(
            *entryInfo
        ))

def doVerse(node):
    global curVerseNode, curVerseInfo, max_len_text, max_len_xml
    if curVerseNode != None:
        thisText = ''.join('{}{}'.format(x[0], x[1]) for x in curVerseInfo)
        thisXml = ''.join(
            '''<w m="{}" t="{}" l="{}">{}</w>'''.format(
                x[2], x[1].replace('\n', '&#xa;'), x[4], x[0]
            ) for x in curVerseInfo)
        fieldSizes['verse']['text'] = max((len(thisText), fieldSizes['verse']['text']))
        fieldSizes['verse']['xml'] = max((len(thisXml), fieldSizes['verse']['xml']))
        tables['verse'].append("({},{},{},{},{},'{}','{}')".format(
            curId['verse'], 
            curVerseFirstSlot, 
            curVerseLastSlot, 
            curId['chapter'], F.verse.v(curVerseNode), sEsc(thisText), sEsc(thisXml),
        ))
        for x in curVerseInfo:
            tables['word_verse'].append("({}, {}, '{}')".format(
                x[2], x[3], x[4]
            ))
        curVerseInfo = []
    curVerseNode = node    

for node in N():
    otype = Fotypev(node)
    if otype == 'word':
        if node in qeres:
            (text, trailer) = qeres[node]
        else:
            text = Ftextv(node)
            trailer = Ftrailerv(node)
        if trailer.endswith('ס') or trailer.endswith('פ'): trailer += ' '
        lex = Flexv(node)
        lang = Flanguagev(node)
        lid = lexIndex.get((lang, lex), None)
        if lid == None:
            lexNotFound[(lang, lex)][Foccv(node)] += 1
        curVerseInfo.append((
            text,
            trailer,
            node, 
            curId['verse'],
            lid,
        ))
    elif otype == 'verse':
        doVerse(node)
        slots = L.d(node, otype='word')
        curId['verse'] += 1
        curVerseFirstSlot = slots[0]
        curVerseLastSlot = slots[-1]
    elif otype == 'chapter':
        doVerse(None)
        slots = L.d(node, otype='word')
        curId['chapter'] += 1
        tables['chapter'].append("({},{},{},{},{})".format(
            curId['chapter'], slots[0], slots[-1], curId['book'], F.chapter.v(node),
        ))
    elif otype == 'book':
        doVerse(None)
        slots = L.d(node, otype='word')
        curId['book'] += 1
        name = F.book.v(node)
        fieldSizes['book']['name'] = max((len(name), fieldSizes['book']['name']))
        tables['book'].append("({},{},{},'{}')".format(
            curId['book'], slots[0], slots[-1], sEsc(name),
        ))
    elif otype == 'clause_atom':
        curId['clause_atom'] += 1
        slots = L.d(node, otype='word')
        ca_num = Fnumberv(node)
        wordtexts = []
        for w in L.d(node, otype='word'):
            trsep = Ftrailerv(w)
            if trsep.endswith('ס') or trsep.endswith('פ'): trsep += ' '
            wordtexts.append(F.g_word_utf8.v(w) +trsep)
        text = ''.join(wordtexts)
        fieldSizes['clause_atom']['text'] = max((len(text), fieldSizes['clause_atom']['text']))
        tables['clause_atom'].append("({},{},{},{},{},'{}')".format(
            curId['clause_atom'], slots[0], slots[-1], ca_num, curId['book'], sEsc(text),
        ))
doVerse(None)

for tb in sorted(fieldLimits):
    for fl in sorted(fieldLimits[tb]):
        limit = fieldLimits[tb][fl]
        actual = fieldSizes[tb][fl]
        exceeded = actual > limit
        utils.caption(0, '{:<5} {:<15}{:<15}: max size = {:>7} of {:>5}'.format(
            'ERROR' if exceeded else 'OK',
            tb, fl, actual, limit,
        ))

utils.caption(0, 'Done')
if len(lexNotFound):
    utils.caption(0, 'Text lexemes not found in lexicon: {}x'.format(len(lexNotFound)))
    for l in sorted(lexNotFound):
        utils.caption(0, '{} {}'.format(*l))
        for (o, n) in sorted(lexNotFound[l].items(), key=lambda x: (-x[1], x[0])):
            utils.caption(0, '\t{}: {}x'.format(o, n))
else:
    print('All lexemes have been found in the lexicon')


# # Fill the word info table with data

# In[14]:


targetTypes = {
    'sentence', 'sentence_atom', 
    'clause', 'clause_atom', 
    'phrase', 'phrase_atom', 
    'subphrase',
}

def ranges(slotSet):
    result = []
    curStart = None
    curEnd = None
    for i in sorted(slotSet):
        if curStart == None:
            curStart = i
            curEnd = i
        else:
            if i == curEnd + 1:
                curEnd += 1
            else:
                result.append((curStart, curEnd))
                curStart = i
                curEnd = i
    if curStart != None:
        result.append((curStart, curEnd))
    return result

def getObjects(vn):
    objects = set()
    for wn in L.d(vn, otype='word'):
        objects.add(wn)
        for tt in targetTypes:
            for on in L.u(wn, otype=tt):
                objects.add(on)
    return objects


# In[15]:


utils.caption(4, 'Generating word info data ...')
tables['word'] = []

if 'word' in fieldSizes: del fieldSizes['word']

def doVerseInfo(verse):
    slots = L.d(verse, otype='word')
    
    (verseStartSlot, verseEndSlot) = (slots[0], slots[-1])
    objects = getObjects(verse)
    words = [dict() for i in range(verseStartSlot, verseEndSlot + 1)]
    for w in words:
        for (otype, doBorder) in (
            ('sentence', True), 
            ('sentence_atom', False), 
            ('clause', True), 
            ('clause_atom', False), 
            ('phrase', True),
            ('phrase_atom', False),
            ('subphrase', True),
            ('word', False),
        ):
            w['{}_{}'.format(otype, 'number')] = list()
            if doBorder:
                w['{}_{}'.format(otype, 'border')] = set()
    nWords = len(words)
    subphraseCounter = 0
    wordNodes = []
    for n in objects:
        otype = F.otype.v(n)
        if otype == 'word': wordNodes.append(n)
        numberProp = '{}_{}'.format(otype, 'number')
        if otype != 'word' and not otype.endswith('_atom'):
            borderProp = '{}_{}'.format(otype, 'border')
        else:
            borderProp = None

        if otype == 'subphrase': subphraseCounter += 1
        elif otype in {'phrase', 'clause', 'sentence'}: subphraseCounter = 0
# Here was a bug: I put the subphraseCounter to 0 upon encountering anything else than a subphrase or a word.
# I had overlooked the half_verse, which can cut through a phrase
        thisInfo = {}
        thisNumber = None
        for f in wordFields:
            (method, name, typ) = (f[0], '{}_{}'.format(f[2], f[1]), f[3])
            if otype != f[2] or method == None: continue
            if method == 'id':
                value = subphraseCounter
            else:
                value = method(n)
                if typ == 'int': value = int(value)
            if name == numberProp:
                thisNumber = value
            else:
                thisInfo[name] = value
        if otype == 'word':
            target = words[thisNumber - verseStartSlot]
            target.update(thisInfo)
            target[numberProp].append(thisNumber)            
        else:
            theseRanges = ranges(L.d(n, otype='word'))
            nRanges = len(theseRanges) - 1
            for (e,r) in enumerate(theseRanges):
                isFirst = e == 0
                isLast = e == nRanges
                rightBorder = 'rr' if isFirst else 'r'
                leftBorder = 'll' if isLast else 'l'
                firstWord = -1 if r[0] < verseStartSlot else nWords if r[0] > verseEndSlot else r[0] - verseStartSlot
                lastWord = -1 if r[1] < verseStartSlot else nWords if r[1] > verseEndSlot else r[1] - verseStartSlot
                myFirstWord = max(firstWord, 0)
                myLastWord = min(lastWord, nWords - 1)
                for i in range(myFirstWord, myLastWord + 1):
                    target = words[i]
                    if not firstOnly[numberProp] or i == myFirstWord:
                        target[numberProp].append(thisNumber)
                    for f in thisInfo:
                        if not firstOnly[name] or i == myFirstWord:
                            target[f] = thisInfo[f]
                    if otype == 'subphrase':
                        if borderProp != None: words[i][borderProp].add('sy')
                if 0 <= firstWord < nWords:
                    if borderProp != None: words[firstWord][borderProp].add(rightBorder)
                if 0 <= lastWord < nWords:
                    if borderProp != None: words[lastWord][borderProp].add(leftBorder)
    wordtext = []
    for w in wordNodes:
        trsep = Ftrailerv(w)
        if trsep.endswith('ס') or trsep.endswith('פ'): trsep += ' '
        wordtext.append(F.g_word_utf8.v(w) +trsep)
    for w in words:
        row = []
        rrow = []
        for f in wordFields:
            typ = f[3]
            name = '{}_{}'.format(f[2], f[1])
            value = w.get(name, 'NULL' if typ == 'int' else '')
            if f[1] == 'border':
                value = ' '.join(value)
            elif f[1] == 'number':
                value = ' '.join(str(v) for v in value)
            rrow.append(str(value).replace('\n', '\\n').replace('\t', '\\t'))
            if typ == 'int':
                value = str(value)
            else:
                if typ.endswith('char'):
                    lValue = len(value)
                    curlen = fieldSizes['word'][name]
                    if lValue > curlen: fieldSizes['word'][name] = lValue
                value = "'{}'".format(sEsc(value))
            row.append(value)
        tables['word'].append('({})'.format(','.join(row)))

for n in N():
    if F.otype.v(n) == 'book':
        utils.caption(0, '\t{}'.format(F.book.v(n)))
    elif F.otype.v(n) == 'verse':
        doVerseInfo(n)

utils.caption(0, 'Done')


# In[16]:


# check whether the field sizes are not exceeded

tb = 'word'
for f in wordFields:
    (fl, typ, limit) = ('{}_{}'.format(f[2], f[1]), f[3], f[4])
    if typ != 'varchar': continue
    actual = fieldSizes[tb][fl]
    exceeded = actual > limit
    outp = sys.stderr if exceeded else sys.stdout
    outp.write('{:<5} {:<15}{:<20}: max size = {:>7} of {:>5}\n'.format(
        'ERROR' if exceeded else 'OK',
        tb, fl, actual, limit,
    ))


# # SQL generation

# In[17]:


limitRow = 2000

tablesHead = collections.OrderedDict((
    ('book', 'insert into book (id, first_m, last_m, name) values \n'),
    ('chapter', 'insert into chapter (id, first_m, last_m, book_id, chapter_num) values \n'),
    ('verse', 'insert into verse (id, first_m, last_m, chapter_id, verse_num, text, xml) values \n'),
    ('clause_atom', 'insert into clause_atom (id, first_m, last_m, ca_num, book_id, text) values \n'),
    ('lexicon', 'insert into lexicon ({}) values \n'.format(', '.join(f[0] for f in lexFields))),
    ('word', 'insert into word ({}) values \n'.format(', '.join('{}_{}'.format(f[2], f[1]) for f in wordFields))),
    ('word_verse', 'insert into word_verse (anchor, verse_id, lexicon_id) values \n'),
))

sqf = open(mysqlFile, 'w')
sqf.write(textCreateSql)

utils.caption(4, 'Generating SQL ...')
for table in tablesHead:
    utils.caption(0, '\ttable {}'.format(table))
    start = tablesHead[table]
    rows = tables[table]
    r = 0
    while r < len(rows):
        sqf.write(start)
        s = min(r + limitRow, len(rows))
        sqf.write(' {}'.format(rows[r]))
        if r + 1 < len(rows):
            for t in rows[r + 1:s]: sqf.write('\n,{}'.format(t))
        sqf.write(';\n')
        r = s

sqf.close()
utils.caption(0, 'Done')


# # Deliver

# In[18]:


utils.gzip(mysqlFile, mysqlZFile)


# In[ ]:




