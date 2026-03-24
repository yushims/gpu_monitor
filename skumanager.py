# pylint: disable=duplicate-code
# pylint: disable=invalid-name
"""Defines functions that describe our training cluster workspaces."""

# pylint: disable=duplicate-code

vc_to_sub_rg_ws_dict = {
    # (subscription id, resource group name, workspace name, uai name)
    "spch-sing-am-e2e-eu": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-am",
        "speech-sing-e2e-ws01-eastus",
        "speech-sing-am-uai",
    ),
    "spch-sing-am-e2e-sc": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-am",
        "speech-sing-e2e-ws01-scus",
        "speech-sing-am-uai",
    ),
    "spch-sing-de-prod-wu2": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-de",
        "speech-sing-de-prod-ws01-westus2",
        "speech-sing-de-uai",
    ),
    "spch-sing-devtest-eu": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-dev-test",
        "speech-sing-dev-test-ws01-eastus",
        "speech-sing-dev-test-uai",
    ),
    "spch-sing-wu3": (
        "b73d8cb2-6e57-4dc7-8775-0e125652753a",
        "speech-sing",
        "speech-sing-ws01-westus3",
        "speech-sing-uai",
    ),
    "cogs-sing-shared-eu": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "cogsvc-sing",
        "cogsvc-sing-ws01-eastus",
        "cogsvc-sing-uai",
    ),
    "cogs-sing-shared-sc": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "cogsvc-sing",
        "cogsvc-sing-ws01-scus",
        "cogsvc-sing-uai",
    ),
    "nuance-sing-acs-wu2": (
        "8a3a6741-510e-4aca-8e8b-f9296ebb9812",
        "nuance-sing-acs",
        "nuance-sing-acs-ws01-westus2",
        "nuance-sing-acs-uai",
    ),
    "cogsvc-sing-amd-vc02": (
        "b73d8cb2-6e57-4dc7-8775-0e125652753a",
        "cogsvc-sing-amd-vc02",
        "cogsvc-sing-amd-vc02-ws-wu3",
        "cogsvc-sing-amd-vc02-uai",
    ),
    "spch-sing-prod-sc": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing",
        "speech-sing-prod-ws01-scus",
        "speech-sing-uai",
    ),
    "spch-sing-prod-eu": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing",
        "speech-sing-prod-ws01-eastus",
        "speech-sing-uai",
    ),
    "spch-sing-prod-wu2": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing",
        "speech-sing-prod-ws01-westus2",
        "speech-sing-uai",
    ),
    # TTS clusters
    "spch-sing-tts-sc": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-tts",
        "speech-sing-tts-ws01-scus",
        "speech-sing-tts-uai",
    ),
    "spch-sing-ttsprod-sc": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-tts",
        "speech-sing-tts-prod-ws01-scus",
        "speech-sing-tts-uai",
    ),
    "spch-sing-tts-wu2": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-tts",
        "speech-sing-tts-ws01-westus2",
        "speech-sing-tts-uai",
    ),
    "spch-singttsprod-wu2": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-tts",
        "speech-sing-tts-prod-ws01-westus2",
        "speech-sing-tts-uai",
    ),
    # End of TTS clusters
    "cogsvc-a100-uksouth": (
        "42ae47bd-b19b-42c1-b0b9-19fd5be9d51b",
        "speech",
        "speech-a100-ws03",
        "speech-uai",
    ),
    "spch-sing-lmres-wu2": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "speech-sing-lmteam",
        "speech-sing-lmres-ws01-westus2",
        "speech-sing-lmteam-uai",
    ),
    "heron-cogsvc-a100-uksouth": (
        "08047947-f71e-4462-a09d-266e3d34c431",
        "EYESON.HERON.PROD.f73bdcf7-bcad-4070-b05e-e8a336edf759",
        "amlworkspaceo4thvhvdbigb2",
        "",
    ),
    "haier-external-restricted-ws-scus": (
        "0d0c25c2-20b7-4640-97ee-826e66e341ca",
        "Haier-External-Restricted",
        "haier-external-restricted-ws-scus",
        "Haier-External-Restricted-uai",
    ),
    "ast-sing-prod01-eus": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "ast-singularity-01",
        "ast-sing-prod-01-ws01-eus",
        "ast-singularity-01-uai",
    ),
    "ast-sing-prod02-eus": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "ast-singularity-02",
        "ast-sing-prod-02-ws01-eus",
        "ast-singularity-02-uai",
    ),
    "ast-sing-debug-eu": (
        "48b6cd5e-3ffe-4c2e-9e99-5760a42cd093",
        "ast-sing-debug",
        "ast-debug-prod-ws01-eus",
        "ast-sing-debug-uai-eus",
    ),
    "spch-tts-wu3": (
        "5c9e4789-4852-4ffe-8551-d682affcbd74",
        "speech-tts-rg",
        "speech-tts-ws01-wus3",
        "speech-tts-rg-uai",
    ),
    "spch-train-h200-safn": (
        "f0d830dc-9b44-446e-86dd-722e7de7c533",
        "speech-training-h200",
        "speech-training-prod-ws01",
        "speech-training-uai-safn",
    ),
    # AI service cluster
    "aiservices-vc01-eu2": (
        "06c76609-9f7e-4814-bf7c-0c5916b9ad75",
        "aiservices-training-c01",
        "aiservices-prod-ws01-eus2",
        "aiservices-training-c01-uai-eus2",
    ),
}

FIPS_VCS = {"ast-sing-prod01-eus", "spch-tts-wu3"}


def get_data_storage_region(data_storage_account, vc=None):
    """Get region for storage account."""
    # This is a weird condition where "rrlab" must be the compute region
    # but the data/workspace is in WUS2
    if vc == "cogsvc-sing-amd-vc01":
        return "rrlab"
    if vc.endswith("-uksouth"):
        return "uksouth"
    if data_storage_account in ("lmcrdata", "nuancetsstd01wus2"):
        return "westus2"
    region_dict = {
        "eus": "eastus",
        "eus2": "eastus2",
        "scus": "southcentralus",
        "wus2": "westus2",
        "wus3": "westus3",
        "safn": "southafricanorth",
        "rrlab": "rrlab",
    }

    # Assert "prmblob" or "stdstoragetts" is in the storage account name
    assert any(
        list(x in data_storage_account for x in ["prmblob", "stdstoragetts", "highperf"])
    ), f"Storage account name {data_storage_account} is not valid"
    # Get the region from the storage account name
    region = ""
    if "prmblob" in data_storage_account:
        region = data_storage_account.split("prmblob")[1]
    elif "stdstoragetts" in data_storage_account:
        region = data_storage_account.split("stdstoragetts")[1]
    elif "highperf" in data_storage_account:
        region = data_storage_account.split("highperf")[1]
    # drop number
    region = region[2:]
    # verify known region
    assert region in region_dict
    return region_dict[region]


# TODO: switch to Azure SDK lookup instead of table lookup/hardcoding
def get_model_registry_region(model_storage_account):
    """Get region of AML model registry to be used."""
    region = None
    if "tsstd01" in model_storage_account:
        region = model_storage_account.split("tsstd01")[1]
    elif "nuancetsstd01" in model_storage_account:
        region = model_storage_account.split("nuancetsstd01")[1]
    elif "exawattaiprmbtts01" in model_storage_account:
        region = model_storage_account.split("exawattaiprmbtts01")[1]
    elif "highperf01" in model_storage_account:
        region = model_storage_account.split("highperf01")[1]

    region_dict = {
        "eus": "eastus",
        "eus2": "eastus2",
        "scus": "southcentralus",
        "wus2": "westus2",
        "wus3": "westus3",
        "safn": "southafricanorth",
        "rrlab": "westus2",
        "uks": "uksouth",
    }

    return region_dict[region] if region else None


def get_data_storage_account_by_cluster_name(cluster_name):
    """Get associated storage account for a cluster.

    /datablob maps to the data container on the regionalized account (highperf01).

    Args:
        cluster_name (str): The name of the VC.
    """
    cluster_region = cluster_name.split("-")[-1]
    cluster_region_to_data_storage_accounts_dict = {
        "wu2": "highperf01wus2",
        "eu": "highperf01eus",
        "eus": "highperf01eus",
        "sc": "highperf01scus",
        "wu3": "highperf01wus3",
        "eu2": "highperf01eus2",
        "safn": "highperf01safn",
    }

    cluster_with_ambigious_region_to_data_storage_accounts_dict = {
        "cogsvc-sing-amd-vc01": "highperf01wus2",
        "cogsvc-sing-amd-vc02": "highperf01wus3",
        "cogsvc-a100-uksouth": "tsprmblob01uks",
        "spch-sing-lmres-wu2": "lmcrdata",
        # NOTE: these are standard storage accounts in EUS2, so expect performance issues
        "heron-cogsvc-a100-uksouth": "amlstorageo4thvhvdbigb2",
        "haier-external-restricted-ws-scus": "highperf01scus",  # datablob
        # TTS clusters
        "spch-sing-tts-sc": "stdstoragetts01scus",
        "spch-sing-ttsprod-sc": "stdstoragetts01scus",
        "spch-sing-tts-wu2": "stdstoragetts01wus2",
        "spch-singttsprod-wu2": "stdstoragetts01wus2",
        # End of TTS clusters
        "nuance-sing-acs-wu2": "nuancetsstd01wus2",
    }

    data_storage_account = cluster_with_ambigious_region_to_data_storage_accounts_dict.get(cluster_name, None)
    # do the ambiguous check first in case it ends with an existing region
    if not data_storage_account:
        data_storage_account = cluster_region_to_data_storage_accounts_dict.get(cluster_region, None)
        if not data_storage_account:
            raise ValueError(f"Cannot find the data storage account for cluster {cluster_name}")

    return data_storage_account


def get_model_storage_account_by_cluster_name(cluster_name):
    """Get associated storage account for a cluster."""
    cluster_region = cluster_name.split("-")[-1]
    cluster_region_to_model_storage_account_dict = {
        "wu2": "highperf01wus2",
        "eu": "highperf01eus",
        "eus": "highperf01eus",
        "sc": "highperf01scus",
        "wu3": "highperf01wus3",
        "eu2": "highperf01eus2",
        "safn": "highperf01safn",
    }

    cluster_with_ambigious_region_to_model_storage_account_dict = {
        "cogsvc-sing-amd-vc01": "highperf01wus2",
        "cogsvc-sing-amd-vc02": "highperf01wus3",
        "cogsvc-a100-uksouth": "tsstd01uks",
        "heron-cogsvc-a100-uksouth": "amlstorageo4thvhvdbigb2",
        "haier-external-restricted-ws-scus": "externaldata3p",
        "nuance-sing-acs-wu2": "nuancetsstd01wus2",
        "spch-sing-tts-sc": "exawattaiprmbtts01scus",
        "spch-sing-ttsprod-sc": "exawattaiprmbtts01scus",
        "spch-sing-tts-wu2": "exawattaiprmbtts01wus2",
        "spch-singttsprod-wu2": "exawattaiprmbtts01wus2",
    }

    # do the ambigious check first in case it ends with an existing region
    model_storage_account = cluster_with_ambigious_region_to_model_storage_account_dict.get(cluster_name, None)

    if not model_storage_account:
        model_storage_account = cluster_region_to_model_storage_account_dict.get(cluster_region, None)
        if not model_storage_account:
            raise ValueError(f"Cannot find the data storage account for cluster {cluster_name}")

    return model_storage_account


def get_non_highperf_data_storage_accounts():
    """Get the standard storage account for data storage."""
    storage_accounts = [
        "lmcrdata",
        "amlstorageo4thvhvdbigb2",
        "nuancetsstd01wus2",
        # TTS data storage accounts
        "stdstoragetts01scus",
        "stdstoragetts01wus2",
        # End of TTS data storage accounts
    ]
    return storage_accounts


# pylint: disable=invalid-name
def get_vc_sub_rg_workspace(vc: str = None) -> tuple:
    """Get associated subscription, resource group, workspace and uai for a given
    Singularity VC
    Args:
        vc (str): The name of the VC.  If None, return all VCs.
    Returns:
        tuple: (subscription id, resource group name, workspace name, uai name)
    """
    if vc in vc_to_sub_rg_ws_dict:
        return vc_to_sub_rg_ws_dict[vc]
    raise ValueError(f"Cannot find the workspace scope for cluster {vc}")


def get_vc_sub_rg_workspace_dict():
    """
    Returns the dict mapping from a VC to subscription, resource group, and workspace.
    Needed for `list` in `aml_cli_cmds.py`
    """
    return vc_to_sub_rg_ws_dict


def get_sku_detail_by_instance_type(instance_type):
    """Get SKU details by Instance type (Series). This is only for singularity"""

    sku_detail_dict = {
        # (GPU, Memory, GPU/Node, IB)
        "NCv3": ("V100", 16, 4, True),
        "NDv2g1": ("V100", 16, 8, False),
        "NDv2": ("V100", 32, 8, True),
        "NDAMv4": ("A100", 80, 8, True),
        "NC_A100_v4": ("A100", 80, 4, False),
        "NDv4": ("A100", 40, 8, True),
        "NDMI200v4": ("MI200", 64, 16, True),
        "MI100": ("MI100", 32, 8, True),
        "ND96amsr_A100_v4": ("A100", 80, 8, True),
        "NDH200v5": ("H200", 141, 8, True),
        "NDH100v5": ("H100", 80, 8, True),
    }

    if instance_type in sku_detail_dict:
        return sku_detail_dict[instance_type]
    raise ValueError(f"Cannot find sku_detail for instance {instance_type}")


def get_instance_type_by_sku(sku, gpu_count, is_ib):
    """Get instance type by SKU"""
    instance_type_by_sku_dict = {
        # (GPU, Memory, #GPU/Node, IB)
        "NCv3": {
            "NC6_v3": ("V100", 16, 1, False),
            "NC12_v3": ("V100", 16, 2, False),
            "NC24_v3": ("V100", 16, 4, False),
            "NC24r_v3": ("V100", 16, 4, True),
        },
        "NDv2g1": {
            "ND5_v2g1": ("V100", 16, 1, False),
            "ND10_v2g1": ("V100", 16, 2, False),
            "ND20_v2g1": ("V100", 16, 4, False),
            "ND40s_v2g1": ("V100", 16, 8, False),
        },
        "NDv2": {
            "ND5_v2": ("V100", 32, 1, False),
            "ND10_v2": ("V100", 32, 2, False),
            "ND20_v2": ("V100", 32, 4, False),
            "ND40r_v2": ("V100", 32, 8, True),
            "ND40rs_v2": ("V100", 32, 8, True),
        },
        "NDAMv4": {
            "ND12am_A100_v4": ("A100", 80, 1, False),
            "ND24am_A100_v4": ("A100", 80, 2, False),
            "ND48am_A100_v4": ("A100", 80, 4, False),
            "ND96amr_A100_v4": ("A100", 80, 8, True),
            "ND96amrs_A100_v4": ("A100", 80, 8, True),
        },
        "NC_A100_v4": {
            "NC24ad_A100_v4": ("A100", 80, 1, False),
            "NC48ad_A100_v4": ("A100", 80, 2, False),
            "NC96ad_A100_v4": ("A100", 80, 4, False),
        },
        "NDv4": {
            "ND12_v4": ("A100", 40, 1, False),
            "ND24_v4": ("A100", 40, 2, False),
            "ND48_v4": ("A100", 40, 4, False),
            "ND96r_v4": ("A100", 40, 8, True),
            "ND96rs_v4": ("A100", 40, 8, True),
        },
        "NDMI200v4": {
            "ND12as_MI200_v4": ("MI200", 64, 2, False),
            "ND24as_MI200_v4": ("MI200", 64, 4, False),
            "ND48as_MI200_v4": ("MI200", 64, 8, False),
            "ND96asr_MI200_v4": ("MI200", 64, 16, True),
            "ND96as_MI200_v4": ("MI200", 64, 16, False),
        },
        "MI100": {
            "MI100_1": ("MI100", 32, 1, False),
            "MI100_2": ("MI100", 32, 2, False),
            "MI100_4": ("MI100", 32, 4, False),
            "MI100_8": ("MI100", 32, 8, True),
        },
        "ND96amsr_A100_v4": {
            "ND96amsr_A100_v4": ("A100", 80, 8, True),
        },
        "NDH200v5": {
            "ND12r_H200_v5": ("H200", 141, 1, True),
            "ND12_H200_v5": ("H200", 141, 1, False),
            "ND24r_H200_v5": ("H200", 141, 2, True),
            "ND24_H200_v5": ("H200", 141, 2, False),
            "ND48r_H200_v5": ("H200", 141, 4, True),
            "ND48_H200_v5": ("H200", 141, 4, False),
            "ND96r_H200_v5": ("H200", 141, 8, True),
            "ND96r_H200_v5-n1": ("H200", 141, 8, True),
            "ND96_H200_v5": ("H200", 141, 8, False),
            "ND96_H200_v5-n1": ("H200", 141, 8, False),
        },
        "NDH100v5": {
            "ND12_H100_v5": ("H100", 80, 1, False),
            "ND24_H100_v5": ("H100", 80, 2, False),
            "ND48_H100_v5": ("H100", 80, 4, False),
            "ND96_H100_v5": ("H100", 80, 8, False),
            "ND96r_H100_v5": ("H100", 80, 8, True),
        },
    }

    sku_instance_types = instance_type_by_sku_dict[sku]
    for instance_type, instance_type_quotas in sku_instance_types.items():
        _, _, instance_gpu_count, ib_enabled = instance_type_quotas
        if instance_gpu_count >= gpu_count and is_ib and ib_enabled:
            return instance_type
        if instance_gpu_count >= gpu_count:
            return instance_type
    raise ValueError(f"Could not find specific instance type for sku {sku} with given constraints.")
