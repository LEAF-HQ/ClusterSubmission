def ensureDirectory(dirname, use_se=False):
    """Make directory if it does not exist."""
    import os, subprocess
    from printing_utils import yellow  # type: ignore

    if use_se:
        command = "LD_LIBRARY_PATH='' PYTHONPATH='' gfal-mkdir -p %s" % (dirname)
        DEVNULL = open(os.devnull, "wb")
        p = subprocess.Popen(command, stdout=DEVNULL, stderr=DEVNULL, shell=True)
        p.wait()
        DEVNULL.close()
    else:
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        if not os.path.exists(dirname):
            print(yellow('--> failed to make directory "%s"' % (dirname)))
