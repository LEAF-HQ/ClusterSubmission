import os


class SiteSpecificSettings:
    def __init__(self, site):
        if "psi" in site.lower():
            self.psi()
        elif "iihe" in site.lower():
            self.iihe()
        elif "lxplus" in site.lower():
            self.lxplus()
        else:
            raise AttributeError(f"Unknown site {site}. Please implement.")

    def Get(self, name, default=None):
        return getattr(self, name) if hasattr(self, name) else default

    def psi(self):
        self.cluster = "slurm_psi"
        self.do_transfer = True
        self.do_copy = True
        self.tmp_output_folder = f"/scratch/{os.getenv('USER')}/pyRATOutputTemp"
        self.se_director = "root://t3dcachedb03.psi.ch/"
        self.use_se_director = True
        self.copy_command = "LD_LIBRARY_PATH='' PYTHONPATH='' gfal-copy --force"
        self.remove_command = "LD_LIBRARY_PATH='' PYTHONPATH='' gfal-rm"

    def lxplus(self):
        self.cluster = "htcondor_lxplus"
        self.do_transfer = False
        self.do_copy = False
        self.tmp_output_folder = None
        self.se_director = None
        self.use_se_director = False
        self.copy_command = "cp"
        self.remove_command = "rm"

    def iihe(self):
        self.cluster = "htcondor_ulb"
        self.do_transfer = True
        self.do_copy = True
        self.tmp_output_folder = f"{os.getenv('PYRAT_PATH')}/pyRATOutputTemp"
        self.se_director = "davs://maite.iihe.ac.be:2880/"
        self.use_se_director = True
        self.copy_command = "LD_LIBRARY_PATH='' PYTHONPATH='' gfal-copy --force"
        self.remove_command = "LD_LIBRARY_PATH='' PYTHONPATH='' gfal-rm"
