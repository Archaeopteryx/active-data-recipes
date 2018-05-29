from __future__ import print_function, absolute_import, division

from ..cli import RecipeParser
from ..query import run_query

def run(args):
    parser = RecipeParser('date')

    # Percentage of fastest response times to use between 0 and 100. Slower
    # ones will be ignored. E.g. reclassifications create slow times (old
    # classification gets deleted).
    parser.add_argument('--percent', type=int, default=95,
                        help="Percentage of fastest response times to use (int, 0..100)")


    # Time in seconds in which the job should be classified.
    parser.add_argument('--response-limit', type=int, default=15*60,
                        help="Time in seconds in which the job should be classified (int)")

    # Maximum time after a push in which a job has to start to be taken into
    # account. Used to exclude manually requested jobs (retriggers, backfills)
    # which might not be shown anymore on the jobs watched by the sheriffs
    # because they regard the push as done. Time is in seconds.
    parser.add_argument('--start-delay', type=int, default=4*60*60,
                        help="Maximum time after a push in which a job has to start (int)")

    args = parser.parse_args(args)

    query_args = vars(args)

    PERCENTAGE_TO_USE = query_args["percent"]
    RESPONSE_LIMIT = query_args["response_limit"]
    JOB_START_DELAY_MAX = query_args["start_delay"]
    
    # Get the classifications from Active Data.
    # Web interface: https://activedata.allizom.org/tools/query.html
    # The query is stored in the 'queries' sibling folder in the file
    # classification_time_simple.query
    classifications = next(run_query('classification_time_simple', **query_args))['data']

    # A failed job can be classified or be checked for its intermittance with
    # retriggers. Waiting for those retriggers is not counted against the
    # classification time.
    
    # A job group is the set of all job runs which have the push, platform an
    # job configuration in common. By default, this is 1 (or 0), unless a job
    # gets retriggers or backfilled (or automatically retried, e.g. because the
    # machine got terminated by the machine pool provider - irrelevant here).
    jobGroups = []
    jobGroup = {"push": None, # unique push id
                "jobName": None, # concatenation of platform and test suite config
                "jobs": []}
    for row in range(len(classifications["repo.index"])):
        if jobGroup["push"] is None:
            # Set up first job group.
            jobGroup["push"] = classifications['repo.index'][row]
            jobGroup["jobName"] = classifications['job.type.name'][row]
        elif classifications['repo.index'][row] != jobGroup["push"] or classifications['job.type.name'][row] != jobGroup["jobName"]:
            # Data read contains new job group.
            jobGroups.append(jobGroup)
            # Set up new job group.
            jobGroup = {"push": classifications['repo.index'][row],
                        "jobName": classifications['job.type.name'][row],
                        "jobs": []}
        jobGroup["jobs"].append({# Timestamp of the push
                                 'repo.push.date': classifications['repo.push.date'][row],
                                 # Type of the failure classification, e.g. "intermittent", "fixed by commit"
                                 'failure.notes.failure_classification': classifications['failure.notes.failure_classification'][row],
                                 # Timestamp of the creation of the failure classification (float)
                                 'failure.notes.created': classifications['failure.notes.created'][row],
                                 # Timestamp of the job's start time (int)
                                 'action.start_time': classifications['action.start_time'][row],
                                 # Timestamp of the job's end time (int)
                                 'action.end_time': classifications['action.end_time'][row]})
    jobGroups.append(jobGroup)

    # Ignore each job group which at least one job which has been classified as "fixed by commit".
    jobGroupsFiltered = list(
                        filter(
                        lambda jobGroup:
                        len(
                        list(
                        filter(
                        lambda job:
                          # 1 classification: string; 2+ classifications: list
                          job['failure.notes.failure_classification'] == "fixed by commit" or filter(lambda classification: classification == "fixed by commit",
                          job['failure.notes.failure_classification']
                        ), jobGroup["jobs"])
                        )) == 0,
                        jobGroups))

    # Holds all the response time for failures. The highest ones will get
    # ignored, e.g. for reclassifications.
    classificationTimedeltas = []
    for jobGroup in jobGroupsFiltered:
        jobGroup["jobs"].sort(key=lambda job: job["action.start_time"])
        # lastTimeOk holds the end time of the last job which started before an
        # inactivity gap bigger than RESPONSE_LIMIT
        lastTimeOk = None
        for job in jobGroup["jobs"]:
            if not lastTimeOk:
                lastTimeOk = job["action.end_time"]
            else:
                # RESPONSE_LIMIT threshold in which action must be taken
                if job["action.start_time"] - lastTimeOk <= RESPONSE_LIMIT:
                    lastTimeOk = job["action.end_time"]
                else:
                    # Found a gap
                    break
        # Filter out jobs which have started more than the allowed time after the push
        jobsNormalTime = list(filter(lambda job: job["action.start_time"] - job["repo.push.date"] <= JOB_START_DELAY_MAX, jobGroup["jobs"]))
        jobsNormalTime.sort(key=lambda job: job["action.start_time"])
        jobGroup["jobs"] = jobsNormalTime
        for job in jobsNormalTime:
            # 1 classification: string; 2+ classifications: list
            if isinstance(job["failure.notes.created"], list):
                classificationTime = min(job["failure.notes.created"])
            # only one classification time, float instead of list
            else:
            # RESPONSE_LIMIT threshold in which action must be taken
                classificationTime = job["failure.notes.created"]
            classificationTimedeltas.append(max(0, int(classificationTime) - lastTimeOk))
    # print("classificationTimedeltas:", classificationTimedeltas)
    classificationTimedeltas.sort()
    classificationsToUse = int(round(PERCENTAGE_TO_USE / 100 * len(classificationTimedeltas)))
    if len(classificationTimedeltas) > 0 and classificationsToUse == 0:
        classificationsToUse = 1
    return [["average classification time (s)",
             "limit classification time (s)"],
            [int(round(sum(classificationTimedeltas[0:classificationsToUse]) / classificationsToUse)),
             classificationTimedeltas[classificationsToUse - 1]]]
