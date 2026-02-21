def resolve_ml_job_resources_compute(job):
    resources = getattr(job, "resources", None)
    compute = getattr(job, "compute", None)

    if resources is not None and compute is not None:
        return resources, compute

    jobs_map = getattr(job, "jobs", None)
    if isinstance(jobs_map, dict):
        for step in jobs_map.values():
            if resources is None:
                step_resources = getattr(step, "resources", None)
                if step_resources is not None:
                    resources = step_resources

            if compute is None:
                step_compute = getattr(step, "compute", None)
                if step_compute:
                    compute = step_compute

            if resources is not None and compute is not None:
                break

    return resources, compute


def calc_max_workers(item_count, cap=8):
    return max(1, min(cap, item_count))


def init_status_bucket():
    return {"num_jobs": 0, "num_gpus": 0, "unknown_gpus": 0}


def ensure_team_status_bucket(team_jobs, creater, status):
    if creater not in team_jobs:
        team_jobs[creater] = {}
    if status not in team_jobs[creater]:
        team_jobs[creater][status] = init_status_bucket()
    return team_jobs[creater][status]


def ensure_cluster_status_bucket(cluster_jobs, cluster, status, user_types):
    if cluster not in cluster_jobs:
        cluster_jobs[cluster] = {}
    if status not in cluster_jobs[cluster]:
        cluster_jobs[cluster][status] = {user_type: init_status_bucket() for user_type in user_types}
    return cluster_jobs[cluster][status]


def update_counts(bucket, total_gpus):
    bucket["num_jobs"] += 1
    if total_gpus > 0:
        bucket["num_gpus"] += total_gpus
    else:
        bucket["unknown_gpus"] += 1


def get_user_type(creater, manager_fte_label):
    return "Others" if creater == "Others" else manager_fte_label


def normalize_manager(manager, default_domain):
    if "@" in manager:
        return manager
    return f"{manager}@{default_domain}"
