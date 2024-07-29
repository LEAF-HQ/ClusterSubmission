import os, json
import htcondor  # type: ignore
from printing_utils import red, blue, prettydict  # type: ignore
from Utils import ensureDirectory  # type: ignore
from ClusterSpecificSettings import ClusterSpecificSettings  # type: ignore
from UserSpecificSettings import UserSpecificSettings  # type: ignore


def SubmitListToCondor(
    job_args, executable, outdir=None, Time="00:00:00", JsonInfo={}, deleteInfo=[], jsonName="", JobName=None, user_config=None, debug=False
):
    print(blue("  --> Submitting to htcondor " + str(len(job_args)) + " jobs ..."))
    CB = CondorBase(JobName=JobName if JobName else "_".join(str(executable).split(".")[0:-1]), Time=Time, user_config=user_config)
    CB.CreateJobInfo(executable=executable)
    CB.ModifyJobInfo("outdir", outdir if outdir else os.getcwd() + "/jobout/")
    for name, info in JsonInfo.items():
        CB.ModifyJobInfo(name, info)
    for name in deleteInfo:
        CB.DeleteJobInfo(name)
    if debug:
        CB.ModifyJobInfo("ExtraInfo", [{"arguments": arg} for arg in job_args])
        CB.StoreJobInfo(extraName=jsonName)
        ClusterId = -1
    else:
        ClusterId = CB.SubmitManyJobs(job_args=job_args, jsonName=jsonName)
    return ClusterId


def ResubmitFromJson(jsonName, to_remove=[], deleteInfo=[], debug=False):
    print(blue("  --> Resubmitting from " + jsonName))
    CB = CondorBase()
    CB.LoadJobInfo(jsonName + ".json")
    storeInfo = {}
    for name in deleteInfo + ["ExtraInfo", "ClusterId"]:
        storeInfo[name] = CB.JobInfo[name]
        CB.DeleteJobInfo(name)
    for args in storeInfo["ExtraInfo"].copy():
        if any([x in args["arguments"] for x in to_remove]):
            storeInfo["ExtraInfo"].remove(args)
    job_args = [list(x.values())[0] for x in storeInfo["ExtraInfo"]]
    print(blue("  --> Submitting to htcondor " + str(len(job_args)) + " jobs ..."))
    if len(job_args) == 0:
        return
    jsonName = jsonName.split("/")[-1].replace("JobInfo_", "") + "_resubmit"
    if debug:
        CB.ModifyJobInfo("ExtraInfo", storeInfo["ExtraInfo"])
        CB.StoreJobInfo(extraName=jsonName)
    else:
        ClusterId = CB.SubmitManyJobs(job_args=job_args, jsonName=jsonName)
        return ClusterId


class CondorBase:
    def __init__(self, JobName="test", Memory=2, Disk=1, Time="00:00:00", user_config=None):
        self.JobName = JobName
        self.user_settings = UserSpecificSettings(os.getenv("USER"))
        if user_config is not None:
            self.user_settings = self.user_settings.LoadJSON(user_config)
        self.email = self.user_settings.Get("email")
        self.RequestTimeSettingName, self.Time = ClusterSpecificSettings(self.user_settings.Get("cluster")).getTimeInfo(ref_time=Time)
        self.Memory = str(int(Memory) * 1024)
        self.Disk = str(int(Disk) * 1024 * 1024)
        self.CreateShedd()

    def CreateShedd(self):
        col = htcondor.Collector()
        if self.user_settings.Get("cluster") == "htcondor_lxplus":
            credd = htcondor.Credd()
            credd.add_user_cred(htcondor.CredTypes.Kerberos, None)
        self.schedd = htcondor.Schedd(col.locate(htcondor.DaemonTypes.Schedd))

    def CreateJobInfo(self, executable="", arguments=""):
        if not hasattr(self, "JobInfo"):
            self.JobInfo = {}
        outputname = str(self.JobName) + "_$(ClusterId)_$(ProcId)"
        self.JobInfo = {
            "universe": "vanilla",
            "executable": executable,  # the program to run on the execute node
            "arguments": arguments,  # sleep for 10 seconds
            "JobBatchName": str(self.JobName),  # name of the submitted job
            "outdir": "./joboutput/",  # directory to store log/out/err files from htcondor
            "output": "$(outdir)/" + outputname + ".out",  # storage of stdout
            "error": "$(outdir)/" + outputname + ".err",  # storage of stderr
            "log": "$(outdir)/" + outputname + ".log",  # storage of job log info
            "stream_output": "True",  # should transfer output file during the run.
            "stream_error": "True",  # should transfer error file during the run.
            "request_CPUs": "1",  # requested number of CPUs (cores)
            "ShouldTransferFiles": "NO",
            "request_memory": self.Memory,  # memory in GB
            "request_disk": self.Disk,  # disk space in GB
            "notify_user": self.email,  # send an email to the user if the notification condition is set
            "notification": "Never",  # Always/Error/Done
            "getenv": "True",  # port the local environment to the cluster
            # "when_to_transfer_output": "ON_EXIT_OR_EVICT",  # specify when to transfer the outout back. Not tested yet
            # "Hold": "True",  # Start the job with Hold status
            # "transfer_executable": "False",  # Default True: copy the executable to the cluster. Set to False search the executable on the remote machine
            # "requirements": 'OpSysAndVer == "CentOS7"',  # additional requirements. Not tested yet
            # "+RequestRuntime": str(int(nHours * 60 * 60)),  # requested run time. Not tested yet
        }
        if self.RequestTimeSettingName:
            self.ModifyJobInfo(self.RequestTimeSettingName, self.Time)  # Time requested

    def ModifyJobInfo(self, name, info):
        if not hasattr(self, "JobInfo"):
            self.CreateJobInfo()
        self.JobInfo[name] = info

    def DeleteJobInfo(self, name):
        if not hasattr(self, "JobInfo"):
            self.CreateJobInfo()
        del self.JobInfo[name]

    def SubmitJob(self, extraName=""):
        if self.JobInfo["executable"] == "":
            raise ValueError("No executable passed. Please check")
        ensureDirectory(self.JobInfo["outdir"])
        submit_result = self.schedd.submit(htcondor.Submit(self.JobInfo))  # submit the job
        ClusterId = str(submit_result.cluster())
        ProcId = str(submit_result.first_proc())
        self.ModifyJobInfo("ClusterId", ClusterId)
        self.ModifyJobInfo("ProcId", ProcId)
        self.StoreJobInfo(extraName=extraName + "_" + str(ClusterId) + "_" + str(ProcId))

    def SubmitManyJobs(self, job_args=[], job_exes=[], jsonName=""):
        import warnings

        warnings.filterwarnings("ignore", category=FutureWarning, module="htcondor")
        ensureDirectory(self.JobInfo["outdir"])
        sub = htcondor.Submit(self.JobInfo)
        if len(job_exes) == 0:
            if self.JobInfo["executable"] == "":
                raise ValueError("No executable passed. Please check")
            jobs = [{"arguments": str(arg)} for arg in job_args]
        elif len(job_args) == 0:
            jobs = [{"arguments": "", "executable": str(job_exes[n])} for n in range(len(job_exes))]
        elif len(job_exes) == len(job_args):
            jobs = [{"arguments": str(job_args[n]), "executable": str(job_exes[n])} for n in range(len(job_exes))]
        else:
            raise ValueError(red("Something is wrong in the SubmitManyJobs parameters"))
        ClusterId = -1
        with self.schedd.transaction() as txn:
            submit_result = sub.queue_with_itemdata(txn, 1, iter(jobs))
            ClusterId = submit_result.cluster()
            self.ModifyJobInfo("ClusterId", str(ClusterId))
            self.ModifyJobInfo("ExtraInfo", jobs)
            self.StoreJobInfo(extraName=jsonName)
        warnings.resetwarnings()
        return ClusterId

    def CheckStatus(self):
        for job in self.schedd.xquery(constraint="ClusterId == {}".format(self.JobInfo["ClusterId"])):
            print(repr(job))

    def StoreJobInfo(self, extraName=""):
        if extraName != "":
            extraName = "_" + extraName
        with open(os.path.join(self.JobInfo["outdir"], "JobInfo" + extraName + ".json"), "w") as f:
            json.dump(self.JobInfo, f, sort_keys=True, indent=4)

    def LoadJobInfo(self, filename):
        with open(filename, "r") as f:
            self.JobInfo = json.load(f)

    def PrintJobInfo(self):
        print(blue("--> JobInfo"))
        prettydict(self.JobInfo)
