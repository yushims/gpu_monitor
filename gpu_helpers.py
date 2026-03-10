import ast
import inspect
import textwrap

from skumanager import get_instance_type_by_sku


VC_GPU_MAP = {
    # "ast-sing-prod01-eus": 4,  # A100
    # "aisupercomputer": 4,  # A100
    # "ast-sing-prod02-eus": 1,  # A100
    # "spch-sing-am-e2e-eu": 8,  # V100
    # "spch-sing-am-e2e-sc": 8,  # V100
    # "spch-sing-prod-eu": 8,  # V100
    # "spch-sing-prod-sc": 8,  # V100
    # "spch-sing-prod-wu2": 8,  # V100
    # "spch-train-h200-safn": 8,  # H200
    "batchv100x8x32g": 8,  # Batch V100
    "v100_mark3": 8,  # Batch V100
    "v100_mark2": 8,  # Batch V100
    "nd40rs_v2": 8,  # Batch V100
}


def _build_instance_type_gpu_info():
    source = textwrap.dedent(inspect.getsource(get_instance_type_by_sku))
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "instance_type_by_sku_dict":
                    sku_to_instances = ast.literal_eval(node.value)
                    return {
                        instance_type.lower(): (sku, quotas[2])
                        for sku, instance_type_map in sku_to_instances.items()
                        for instance_type, quotas in instance_type_map.items()
                    }

    return {}


INSTANCE_TYPE_GPU_INFO = _build_instance_type_gpu_info()


def resolve_ml_job_gpus_per_instance(job):
    gpus_per_instance = None
    instance_type = get_ml_instance_type(job)
    if instance_type:
        gpus_per_instance = resolve_instance_type_gpus(instance_type)

    return gpus_per_instance


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


def get_ml_instance_type(job):
    resources = getattr(job, "resources", None)
    if resources:
        instance_type = getattr(resources, "instance_type", None)
        if instance_type:
            return instance_type

        properties = getattr(resources, "properties", None)
        if properties:
            instance_type = properties.get("instance_type", None)
            if instance_type:
                return instance_type

            ai_supercomputer = getattr(properties, "AISuperComputer", None)
            if ai_supercomputer:
                instance_type = ai_supercomputer.get("instanceType", None)
                if instance_type:
                    return instance_type

    properties = getattr(job, "properties", None)
    if properties:
        instance_type = getattr(properties, "azureml.InstanceType", None)
        if instance_type:
            return instance_type

    return None


def resolve_instance_type_gpus(instance_type):
    if instance_type is None:
        return None

    normalized = str(instance_type).strip()
    if normalized.lower().startswith("singularity."):
        normalized = normalized[len("Singularity."):]

    gpu_info = INSTANCE_TYPE_GPU_INFO.get(normalized.lower(), None)
    if gpu_info is None:
        return None

    _, gpus_per_instance = gpu_info
    return gpus_per_instance


def resolve_ml_job_instance_count(job):
    instance_count = None
    resources = getattr(job, "resources", None)
    if resources:
        instance_count = getattr(resources, "instance_count", None)
    return instance_count
