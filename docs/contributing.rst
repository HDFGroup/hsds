Bug Reports & Contributions
===========================

Contributions and bug reports are welcome from anyone!  Some of the best
features in h5py, including thread support, dimension scales, and the
scale-offset filter, came from user code contributions.

Since we use GitHub, the workflow will be familiar to many people.
If you have questions about the process or about the details of implementing
your feature, feel free to ask on Github itself, or on the h5py section of the
HDF5 forum:

    https://forum.hdfgroup.org/c/hdf-tools/h5py

Posting on this forum requires registering for a free account with HDF group.

Anyone can post to this list. Your first message will be approved by a
moderator, so don't worry if there's a brief delay.

This guide is divided into three sections.  The first describes how to file
a bug report.

The second describes the mechanics of
how to submit a contribution to the h5py project; for example, how to
create a pull request, which branch to base your work on, etc.
We assume you're are familiar with Git, the version control system used by h5py.
If not, `here's a great place to start <https://git-scm.com/book>`_.

Finally, we describe the various subsystems inside h5py, and give
technical guidance as to how to implement your changes.


How to File a Bug Report
------------------------

Bug reports are always welcome!  The issue tracker is at:

    https://github.com/h5py/h5py/issues


If you're unsure whether you've found a bug
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Always feel free to ask on the mailing list (h5py at Google Groups).
Discussions there are seen by lots of people and are archived by Google.
Even if the issue you're having turns out not to be a bug in the end, other
people can benefit from a record of the conversation.

By the way, nobody will get mad if you file a bug and it turns out to be
something else.  That's just how software development goes.


What to include
~~~~~~~~~~~~~~~

When filing a bug, there are two things you should include.  The first is
the output of ``h5py.version.info``::

    >>> import h5py
    >>> print(h5py.version.info)

The second is a detailed explanation of what went wrong.  Unless the bug
is really trivial, **include code if you can**, either via GitHub's
inline markup::

    ```
        import h5py
        h5py.explode()    # Destroyed my computer!
    ```

or by uploading a code sample to `Github Gist <http://gist.github.com>`_.

How to Get Your Code into h5py
------------------------------

This section describes how to contribute changes to the h5py code base.
Before you start, be sure to read the h5py license and contributor
agreement in "license.txt".  You can find this in the source distribution,
or view it online at the main h5py repository at GitHub.

The basic workflow is to clone h5py with git, make your changes in a topic
branch, and then create a pull request at GitHub asking to merge the changes
into the main h5py project.

Here are some tips to getting your pull requests accepted:

1. Let people know you're working on something.  This could mean posting a
   comment in an open issue, or sending an email to the mailing list.  There's
   nothing wrong with just opening a pull request, but it might save you time
   if you ask for advice first.
2. Keep your changes focused.  If you're fixing multiple issues, file multiple
   pull requests.  Try to keep the amount of reformatting clutter small so
   the maintainers can easily see what you've changed in a diff.
3. Unit tests are mandatory for new features.  This doesn't mean hundreds
   (or even dozens) of tests!  Just enough to make sure the feature works as
   advertised.  The maintainers will let you know if more are needed.


.. _git_checkout:

Clone the h5py repository
~~~~~~~~~~~~~~~~~~~~~~~~~

The best way to do this is by signing in to GitHub and cloning the
h5py project directly.  You'll end up with a new repository under your
account; for example, if your username is ``yourname``, the repository
would be at http://github.com/yourname/h5py.

Then, clone your new copy of h5py to your local machine::

    $ git clone http://github.com/yourname/h5py


Create a topic branch for your feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check out a new branch for the bugfix or feature you're writing::

    $ git checkout -b newfeature master

The exact name of the branch can be anything you want.  For bug fixes, one
approach is to put the issue number in the branch name.

We develop all changes against the *master* branch.
If we're making a bugfix release, a bot will backport merged pull requests.


Implement the feature!
~~~~~~~~~~~~~~~~~~~~~~

You can implement the feature as a number of small changes, or as one big
commit; there's no project policy.  Double-check to make sure you've
included all your files; run ``git status`` and check the output.

.. _contrib-run-tests:

Run the tests
~~~~~~~~~~~~~

The easiest way to run the tests is with
`tox <https://tox.readthedocs.io/en/latest/>`_::

    pip install tox  # Get tox

    tox -e py37-test-deps  # Run tests in one environment
    tox                    # Run tests in all possible environments
    tox -a                 # List defined environments

Write a release note
~~~~~~~~~~~~~~~~~~~~

Changes which could affect people building and using h5py after the next release
should have a news entry. You don't need to do this if your changes don't affect
usage, e.g. adding tests or correcting comments.

In the ``news/`` folder, make a copy of ``TEMPLATE.rst`` named after your branch.
Edit the new file, adding a sentence or two about what you've added or fixed.
Commit this to git too.

News entries are merged into the :doc:`what's new documents <whatsnew/index>`
for each release. They should allow someone to quickly understand what a new
feature is, or whether a bug they care about has been fixed. E.g.::

    Bug fixes
    ---------

    * Fix reading data for region references pointing to an empty selection.

The *Building h5py* section is for changes which affect how people build h5py
from source. It's not about how we make prebuilt wheels; changes to that which
make a visible difference can go in *New features* or *Bug fixes*.

Push your changes back and open a pull request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Push your topic branch back up to your GitHub clone::

    $ git push origin newfeature

Then, `create a pull request <https://help.github.com/articles/creating-a-pull-request>`_ based on your topic branch.


Work with the maintainers
~~~~~~~~~~~~~~~~~~~~~~~~~

Your pull request might be accepted right away.  More commonly, the maintainers
will post comments asking you to fix minor things, like add a few tests, clean
up the style to be PEP-8 compliant, etc.

The pull request page also shows the results of building and testing the
modified code on Travis and Appveyor CI and Azure Pipelines.
Check back after about 30 minutes to see if the build succeeded,
and if not, try to modify your changes to make it work.

When making changes after creating your pull request, just add commits to
your topic branch and push them to your GitHub repository.  Don't try to
rebase or open a new pull request!  We don't mind having a few extra
commits in the history, and it's helpful to keep all the history together
in one place.


How to Modify h5py
------------------

This section is a little more involved, and provides tips on how to modify
h5py.  The h5py package is built in layers.  Starting from the bottom, they
are:

1. The HDF5 C API (provided by libhdf5)
2. Auto-generated Cython wrappers for the C API (``api_gen.py``)
3. Low-level interface, written in Cython, using the wrappers from (2)
4. High-level interface, written in Python, with things like ``h5py.File``.
5. Unit test code

Rather than talk about the layers in an abstract way, the parts below are
guides to adding specific functionality to various parts of h5py.
Most sections span at least two or three of these layers.


