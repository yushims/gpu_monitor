# python -m pip install azure-identity azure-mgmt-resource azure-mgmt-machinelearningservices azure-ai-ml azure-batch azure-mgmt-batch
# python -m pip install msal requests
# az login
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import time
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.machinelearningservices import AzureMachineLearningWorkspaces
from azure.mgmt.batch import BatchManagementClient
from azure.batch import BatchServiceClient
import requests
from azure.ai.ml import MLClient
from datetime import datetime, timedelta, timezone
import logging
from gpu_helpers import (
    resolve_ml_job_resources_compute,
    calc_max_workers,
    ensure_team_status_bucket,
    ensure_cluster_status_bucket,
    update_counts,
    get_user_type,
    normalize_manager,
)


logging.basicConfig(level=logging.INFO)  # or whatever you like for your own logs
logger = logging.getLogger("gpu_monitor")


def configure_logging(verbose=False):
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    noisy_logger_levels = {
        "azure": logging.ERROR,
        "azureml": logging.ERROR,
        "azure.ai.ml": logging.ERROR,
        "urllib3": logging.WARNING,
        "attr_dict": logging.WARNING,
    }
    for name, level in noisy_logger_levels.items():
        logging.getLogger(name).setLevel(level)


configure_logging(verbose=False)

# -----------------------------
# Configuration
# -----------------------------

credential = DefaultAzureCredential()

DEFAULT_MANAGER_ALIAS = "jinyli"
DEFAULT_MANAGER_DOMAIN = "microsoft.com"
DEFAULT_DAYS_AGO = 21


@dataclass
class RunContext:
    days_ago: int
    cutoff: datetime
    user_alias_map: dict
    manager_fte_label: str
    max_ml_workers: int
    max_batch_workers: int

subscriptions = {
    "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093": "AI Platform GPU 21 - Cognitive Services",
    "f0d830dc-9b44-446e-86dd-722e7de7c533": "AI Platform GPUs - Speech Quality Training",
}

RG_CLUSTER_MAP = {
    "speech-sing-am": "V100",
    "speech-sing": "V100",
    "ast-singularity-01": "A100",
    "ast-singularity-02": "A100",
    "speech-training-h200": "H200",
    "batch_nd40rs_v2": "V100 Batch",
}

# Map resource groups to their type (ML or Batch)
RG_TYPE_MAP = {
    "speech-sing-am": "ml",
    "speech-sing": "ml",
    "ast-singularity-01": "ml",
    "ast-singularity-02": "ml",
    "speech-training-h200": "ml",
    "batch_nd40rs_v2": "batch",
}

VC_GPU_MAP = {
    "ast-sing-prod01-eus": 4,  # A100
    "aisupercomputer": 4,  # A100
    "ast-sing-prod02-eus": 1,  # A100
    "spch-sing-am-e2e-eu": 8,  # V100
    "spch-sing-am-e2e-sc": 8,  # V100
    "spch-sing-prod-eu": 8,  # V100
    "spch-sing-prod-sc": 8,  # V100
    "spch-sing-prod-wu2": 8,  # V100
    "spch-train-h200-safn": 8,  # H200
    "batchv100x8x32g": 8,  # Batch V100
    "v100_mark3": 8,  # Batch V100
    "v100_mark2": 8,  # Batch V100
    "nd40rs_v2": 8,  # Batch V100
}

OTHER_USER_TYPE = "Others"

STATUS_LIST = [
    "Running", 
    "Queued", 
    # "Completed",
]

# map ml job statuses to STATUS_LIST
ML_STATUS_MAP = {
    'Running': 'Running',
    # 'Starting': 'Running',
    # 'Provisioning': 'Running',
    # 'Preparing': 'Running',
    # 'Finalizing': 'Running',
    # 'NotResponding': 'Running',
    'Queued': 'Queued',
    # 'NotStarted': 'Queued',
    # 'CancelRequested': 'Queued',
    # 'Completed': 'Completed',
    # 'Failed': 'Completed',
    # 'Canceled': 'Completed',
}

# map batch job statuses to STATUS_LIST
BATCH_STATUS_MAP = {
    'active': 'Running',
    # 'disabling': 'Running', 
    # 'disabled': 'Completed',
    'enabling': 'Queued',
    # 'terminating': 'Completed',
    # 'completed': 'Completed',
    # 'deleting': 'Completed'
}

class BatchClientAADTokenCredentials:
    """Custom AAD token credentials for Batch client"""
    def __init__(self, credential, resource='https://batch.core.windows.net/'):
        self.credential = credential
        self.resource = resource
        self._token = None
    
    def signed_session(self, session=None):
        if session is None:
            session = requests.Session()
        
        token = self.credential.get_token(self.resource)
        session.headers.update({
            'Authorization': f'Bearer {token.token}'
        })
        return session


# -----------------------------
# Data fetch helpers
# -----------------------------

def get_team_members(manager):
    # get token for Microsoft Graph API
    token = credential.get_token("https://graph.microsoft.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}
    GRAPH = "https://graph.microsoft.com/v1.0"
    select_fields = "displayName,mailNickname,userPrincipalName"

    def graph_get_user(user):
        if not user:
            return None

        req = requests.get(
            f"{GRAPH}/users/{user}",
            headers=headers,
            params={"$select": select_fields}
        )
        req.raise_for_status()
        return req.json()

    def graph_get_reports(manager):
        if not manager:
            return []

        req = requests.get(
            f"{GRAPH}/users/{manager}/directReports",
            headers=headers,
            params={"$select": select_fields}
        )
        req.raise_for_status()

        members = req.json().get("value", [])
        all_members = members.copy()
        for member in members:
            all_members.extend(graph_get_reports(member["userPrincipalName"]))
        return all_members

    manager_info = graph_get_user(manager)
    all_members = ([manager_info] if manager_info else []) + graph_get_reports(manager)

    fte_members = {}
    non_fte_members = {}
    for member in all_members:
        if member["mailNickname"].startswith("v-") or member["mailNickname"].startswith("sc-"):
            non_fte_members[member["displayName"]] = member["mailNickname"]
        else:
            fte_members[member["displayName"]] = member["mailNickname"]
    fte_members = dict(sorted(fte_members.items()))
    non_fte_members = dict(sorted(non_fte_members.items()))

    logger.info("Full-time employees")
    for name, alias in fte_members.items():
        logger.info(f"  {name} ({alias})")
    # print(f"Vendors")
    # for name, alias in non_fte_members.items():
    #     print(f"  {name} ({alias})")

    return fte_members


# get resource groups in a subscription
def get_resource_groups(subscription_id, target_rg_lower=None):
    client = ResourceManagementClient(credential, subscription_id)
    resource_groups = [(rg.name.lower(), rg.name) for rg in client.resource_groups.list()]
    if target_rg_lower:
        resource_groups = [item for item in resource_groups if item[0] == target_rg_lower]
    return resource_groups

# get workspaces in a resource group
def get_workspaces(subscription_id, resource_group):
    ml_mgmt = AzureMachineLearningWorkspaces(credential, subscription_id)
    return [ws.name for ws in ml_mgmt.workspaces.list_by_resource_group(resource_group)]

# get batch accounts in a resource group
def get_batch_accounts(subscription_id, resource_group):
    batch_mgmt = BatchManagementClient(credential, subscription_id)
    return [account.name for account in batch_mgmt.batch_account.list_by_resource_group(resource_group)]

# get pool info from batch client
def get_batch_pool_info(batch_client, pool_id):
    pool = batch_client.pool.get(pool_id)
    return {
        'current_dedicated_nodes': pool.current_dedicated_nodes,
        'current_low_priority_nodes': pool.current_low_priority_nodes,
        'target_dedicated_nodes': pool.target_dedicated_nodes,
        'target_low_priority_nodes': pool.target_low_priority_nodes,
        'vm_size': pool.vm_size
    }

# get jobs in a workspace
def get_jobs(subscription_id, resource_group, workspace_name, cutoff, days=0):
    ml_client = MLClient(
        credential=credential,
        subscription_id=subscription_id,
        resource_group_name=resource_group,
        workspace_name=workspace_name,
    )

    jobs = ml_client.jobs.list()

    for job in jobs:
        if days > 0 and job.creation_context.created_at < cutoff:  # older than cutoff
            break

        job_type = str(getattr(job, "job_type", "")).lower()
        should_query_child_jobs = job_type in {"pipeline", "sweep"}

        has_child_jobs = False
        if should_query_child_jobs:
            child_jobs = ml_client.jobs.list(parent_job_name=job.name)
            for child in child_jobs:
                has_child_jobs = True
                # Map job status to our standard statuses
                mapped_status = ML_STATUS_MAP.get(child.status, child.status)
                if mapped_status in STATUS_LIST:
                    child.creation_context.created_by = child.creation_context.created_by.user_name
                    if not hasattr(child, "resources") or not child.resources:
                        parent_resources, _ = resolve_ml_job_resources_compute(job)
                        setattr(child, "resources", parent_resources)
                    if not hasattr(child, "compute") or not child.compute:
                        _, parent_compute = resolve_ml_job_resources_compute(job)
                        setattr(child, "compute", parent_compute)
                    yield child

        if not has_child_jobs:
            # Map job status to our standard statuses
            mapped_status = ML_STATUS_MAP.get(job.status, job.status)
            if mapped_status in STATUS_LIST:
                yield job

# get batch client for a batch account
def get_batch_client(subscription_id, resource_group, account_name):
    batch_mgmt = BatchManagementClient(credential, subscription_id)
    account = batch_mgmt.batch_account.get(resource_group, account_name)
    account_endpoint = f"https://{account_name}.{account.account_endpoint.split('.', 1)[1]}"

    aad_credentials = BatchClientAADTokenCredentials(credential)
    batch_client = BatchServiceClient(
        aad_credentials,
        batch_url=account_endpoint
    )
    return batch_client

# get jobs in a batch account
def get_batch_jobs(batch_client, cutoff, days=0):
    jobs = batch_client.job.list()

    for job in jobs:
        if days > 0 and job.creation_time < cutoff:  # older than cutoff
            break
            
        mapped_status = BATCH_STATUS_MAP.get(job.state.lower(), job.state)
        if mapped_status in STATUS_LIST:
            job.status = mapped_status
            yield job


# get job creator from ML job
def get_job_creater(job, user_alias_map):
    created_by = job.creation_context.created_by.lower()
    display_name = job.display_name.lower()

    # created_by is in user names
    for creater in user_alias_map.keys():
        if creater.lower() == created_by:
            return creater

    # display_name contains alias
    for creater, alias in user_alias_map.items():
        if alias.lower() in display_name:
            return creater
        
    return "Others"

# get job creator from Batch job
def get_batch_job_creater(batch_client, job, tasks, user_alias_map):
    job_id = job.id.lower()
    display_name = job.display_name.lower()

    assert len(tasks) <= 1, f"More than one task found for Batch job {job.id}. Need further debugging."

    for creater, alias in user_alias_map.items():
        if alias.lower() in job_id:
            return creater

        if alias.lower() in display_name:
            return creater

        for task in tasks:
            # seems not in use in v100_submit.py
            if alias.lower() == task.user_identity.user_name:
                return creater
            
            # only works for running tasks
            # try:
            #     files = list(batch_client.file.list_from_task(job.id, task.id))
            #     for file in files:
            #         if file.name.lower() == alias.lower():
            #             return creater
            # except Exception as e:
            #     continue

    for task in tasks:
        # only works for running tasks
        try:
            files = list(batch_client.file.list_from_task(job.id, task.id))
            for file in files:
                for creater, alias in user_alias_map.items():
                    if file.name.lower() == alias.lower():
                        return creater
        except Exception as e:
            continue

    return "Others"
            
# process ML jobs
def add_ml_job_info(rg, job, creater, team_jobs, cluster_jobs, ctx):
    # get number of instances
    resources, compute = resolve_ml_job_resources_compute(job)
    if resources is None:  # not a model training job
        logger.info(f"      !!!Cannot get resources for job {job.display_name} from {creater}")
        total_gpus = -1
    else:
        instance_count = getattr(resources, "instance_count", None)
        if not instance_count:
            logger.info(f"      !!!Cannot get instance count for job {job.display_name} from {creater}")
            total_gpus = -1
        elif compute is None:  # not a model training job
            logger.info(f"      !!!Cannot get compute for job {job.display_name} from {creater}")
            total_gpus = -1
        else:
            # number of GPUs per instance
            vc = compute.split("/")[-1]
            if vc not in VC_GPU_MAP:
                logger.info(f"      !!!Unknown VC: {vc}, assuming 8 GPUs")
                VC_GPU_MAP[vc] = 8

            total_gpus = VC_GPU_MAP[vc] * instance_count
    logger.info(f"      ML Job of {creater}: {job.creation_context.created_by} | {job.display_name} | {job.creation_context.created_at} | {job.status} | {total_gpus} GPUs")

    team_bucket = ensure_team_status_bucket(team_jobs, creater, job.status)
    update_counts(team_bucket, total_gpus)

    cluster = RG_CLUSTER_MAP[rg]
    cluster_status_bucket = ensure_cluster_status_bucket(cluster_jobs, cluster, job.status, [ctx.manager_fte_label, OTHER_USER_TYPE])
    user_type = get_user_type(creater, ctx.manager_fte_label)
    update_counts(cluster_status_bucket[user_type], total_gpus)

# Helper function to process Batch jobs
def add_batch_job_info(rg, job, tasks, creater, team_jobs, cluster_jobs, ctx):
    # number of GPUs per instance
    pool_id = job.pool_info.pool_id
    if pool_id not in VC_GPU_MAP:
        logger.info(f"      !!!Unknown Pool: {pool_id}, assuming 8 GPUs")
        VC_GPU_MAP[pool_id] = 8

    cluster = RG_CLUSTER_MAP[rg]
    for task in tasks:
        instance_count = task.multi_instance_settings.number_of_instances if task.multi_instance_settings else 1

        total_gpus = VC_GPU_MAP[pool_id] * instance_count
        logger.info(f"      Batch Job of {creater}: {job.id} | {job.display_name} | {job.creation_time} | {job.status} | {total_gpus} GPUs")

        team_bucket = ensure_team_status_bucket(team_jobs, creater, job.status)
        update_counts(team_bucket, total_gpus)

        cluster_status_bucket = ensure_cluster_status_bucket(cluster_jobs, cluster, job.status, [ctx.manager_fte_label, OTHER_USER_TYPE])
        user_type = get_user_type(creater, ctx.manager_fte_label)
        update_counts(cluster_status_bucket[user_type], total_gpus)


def scan_workspace_jobs(subscription_id, rg, rg_lower, ws, ctx):
    started_at = time.perf_counter()
    job_entries = []
    logger.debug(f"    ML Workspace scanning: {ws}")
    for job in get_jobs(subscription_id, rg, ws, cutoff=ctx.cutoff, days=ctx.days_ago):
        if creater := get_job_creater(job, ctx.user_alias_map):
            job_entries.append((rg_lower, job, creater))
            logger.debug(f"      ML Job found [{ws}] {job.status} | {creater} | {job.display_name}")
    elapsed_sec = time.perf_counter() - started_at
    return ws, job_entries, elapsed_sec


def scan_batch_account_jobs(subscription_id, rg, rg_lower, account, ctx):
    batch_client = get_batch_client(subscription_id, rg, account)
    jobs = list(get_batch_jobs(batch_client, cutoff=ctx.cutoff, days=ctx.days_ago))

    pool_infos = {}
    for pool_id in {job.pool_info.pool_id for job in jobs}:
        try:
            pool_infos[pool_id] = get_batch_pool_info(batch_client, pool_id)
        except Exception as exc:
            logger.info(f"    !!!Failed to get pool info for {pool_id}: {exc}")

    job_entries = []
    max_workers = calc_max_workers(len(jobs), cap=ctx.max_batch_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_job = {
            executor.submit(lambda j: list(batch_client.task.list(j.id)), job): job
            for job in jobs
        }
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                tasks = future.result()
                if creater := get_batch_job_creater(batch_client, job, tasks, ctx.user_alias_map):
                    job_entries.append((rg_lower, job, tasks, creater))
            except Exception as exc:
                logger.info(f"      !!!Failed to process batch job {job.id}: {exc}")

    return account, pool_infos, job_entries


# -----------------------------
# Resource-group scanners and summary
# -----------------------------

def scan_ml_resource_group(subscription_id, rg, rg_lower, ctx, team_jobs, cluster_jobs):
    workspaces = get_workspaces(subscription_id, rg)
    if not workspaces:
        return

    max_workers = calc_max_workers(len(workspaces), cap=ctx.max_ml_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ws = {}
        for ws in workspaces:
            logger.debug(f"    ML Workspace queued: {ws}")
            future = executor.submit(scan_workspace_jobs, subscription_id, rg, rg_lower, ws, ctx)
            future_to_ws[future] = ws

        for future in as_completed(future_to_ws):
            ws = future_to_ws[future]
            try:
                ws, job_entries, elapsed_sec = future.result()
                logger.info(f"    ML Workspace done: {ws} ({elapsed_sec:.1f}s)")
                for rg_name, job, creater in job_entries:
                    add_ml_job_info(rg_name, job, creater, team_jobs, cluster_jobs, ctx)
            except Exception as exc:
                logger.info(f"    !!!Failed to scan workspace {ws}: {exc}")


def scan_batch_resource_group(subscription_id, rg, rg_lower, ctx, team_jobs, cluster_jobs):
    batch_accounts = get_batch_accounts(subscription_id, rg)
    if not batch_accounts:
        return

    max_workers = calc_max_workers(len(batch_accounts), cap=ctx.max_batch_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(scan_batch_account_jobs, subscription_id, rg, rg_lower, account, ctx)
            for account in batch_accounts
        ]
        for future in as_completed(futures):
            try:
                account, pool_infos, job_entries = future.result()
                logger.info(f"    Batch Account: {account}")
                for pool_id, pool_info in pool_infos.items():
                    logger.debug(f"      Pool {pool_id}: {pool_info['current_dedicated_nodes']} nodes ({pool_info['vm_size']})")
                for rg_name, job, tasks, creater in job_entries:
                    add_batch_job_info(rg_name, job, tasks, creater, team_jobs, cluster_jobs, ctx)
            except Exception as exc:
                logger.info(f"    !!!Failed to scan batch account: {exc}")


def print_summary(team_jobs, cluster_jobs, ctx):
    logger.info(f"\nSummary of jobs submitted in the last {ctx.days_ago} days (since {ctx.cutoff}):")
    logger.info("--------")

    def creator_total_jobs(creater):
        status_count = team_jobs.get(creater, {})
        return sum(status_count.get(status, {}).get("num_jobs", 0) for status in STATUS_LIST)

    all_creaters = set(team_jobs.keys()) | set(ctx.user_alias_map.keys())
    sorted_creaters = sorted(all_creaters, key=lambda creater: (-creator_total_jobs(creater), creater))

    for creater in sorted_creaters:
        status_count = team_jobs.get(creater, {})
        if not status_count:
            logger.debug(f"  {creater}: 0 jobs")
            continue
        for status in STATUS_LIST:
            if status not in status_count:
                continue
            counts = status_count[status]
            logger.info(f"  {creater}: {counts['num_jobs']} {status} jobs with {counts['num_gpus']} GPUs" + \
                        (f" and {counts['unknown_gpus']} jobs with unknown number of GPUs" if counts['unknown_gpus'] > 0 else ""))

    logger.info("--------")
    for cluster, status_count in cluster_jobs.items():
        logger.info(f"  Cluster: {cluster}")
        for user_type in [ctx.manager_fte_label, OTHER_USER_TYPE]:
            logger.info(f"    {user_type}:")
            for status in STATUS_LIST:
                if status not in status_count:
                    continue
                counts = status_count[status]
                if counts[user_type]['num_gpus'] > 0:
                    logger.info(f"      {counts[user_type]['num_jobs']} {status} jobs with {counts[user_type]['num_gpus']} GPUs")
                if counts[user_type]['unknown_gpus'] > 0:
                    logger.info(f"      {counts[user_type]['unknown_gpus']} {status} jobs with unknown number of GPUs")
    logger.info("--------")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manager", default=DEFAULT_MANAGER_ALIAS)
    parser.add_argument("--days-ago", type=int, default=DEFAULT_DAYS_AGO)
    parser.add_argument("--resource-group", default=None, help="Filter to a single resource group name")
    parser.add_argument("--max-ml-workers", type=int, default=16, help="Maximum ML workspace scan threads")
    parser.add_argument("--max-batch-workers", type=int, default=16, help="Maximum batch scan threads")
    parser.add_argument("--verbose", action="store_true", help="Enable debug-level logging")
    return parser.parse_args()

def main(
    manager=f"{DEFAULT_MANAGER_ALIAS}@{DEFAULT_MANAGER_DOMAIN}",
    days_ago=DEFAULT_DAYS_AGO,
    resource_group=None,
    max_ml_workers=16,
    max_batch_workers=16,
    verbose=False,
):
    configure_logging(verbose=verbose)
    manager_alias = manager.split("@", 1)[0]
    ctx = RunContext(
        days_ago=days_ago,
        cutoff=datetime.now(timezone.utc) - timedelta(days=days_ago),
        user_alias_map=get_team_members(manager),
        manager_fte_label=f"{manager_alias}'s FTE",
        max_ml_workers=max(1, max_ml_workers),
        max_batch_workers=max(1, max_batch_workers),
    )

    rg_cluster_keys_lower = {x.lower() for x in RG_CLUSTER_MAP.keys()}
    rg_type_lower_map = {k.lower(): v for k, v in RG_TYPE_MAP.items()}
    target_rg_lower = resource_group.lower() if resource_group else None

    # Main logic
    team_jobs = {}
    cluster_jobs = {}
    for subscription_id, subscription_name in subscriptions.items():
        logger.info(f"Subscription: {subscription_name}")

        resource_groups = get_resource_groups(subscription_id, target_rg_lower)

        for rg_lower, rg in resource_groups:
            if rg_lower not in rg_cluster_keys_lower:
                continue
            logger.info(f"  Resource Group: {rg}")
            
            # Handle different resource types
            rg_type = rg_type_lower_map[rg_lower]
            
            if rg_type == "ml":
                scan_ml_resource_group(subscription_id, rg, rg_lower, ctx, team_jobs, cluster_jobs)
            
            elif rg_type == "batch":
                scan_batch_resource_group(subscription_id, rg, rg_lower, ctx, team_jobs, cluster_jobs)

    print_summary(team_jobs, cluster_jobs, ctx)


if __name__ == "__main__":
    args = parse_args()
    main(
        manager=normalize_manager(args.manager, DEFAULT_MANAGER_DOMAIN),
        days_ago=args.days_ago,
        resource_group=args.resource_group,
        max_ml_workers=args.max_ml_workers,
        max_batch_workers=args.max_batch_workers,
        verbose=args.verbose,
    )
