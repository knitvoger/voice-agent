"""Create new snapshot-enabled Billing, Booking, and Hub versions; no infrastructure writes."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

from azure.ai.projects import AIProjectClient, models
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

from agent import project_endpoint


def definition(
    endpoint: str, image: str, model: str, role: str, domains: dict[str, Any], *, idle: int = 120, warmup: bool = True
) -> models.HostedAgentDefinition:
    environment = {
        "PROJECT_ENDPOINT": project_endpoint(endpoint),
        "MODEL_DEPLOYMENT": model,
        "AGENT_ROLE": role,
        "DOMAIN_MODEL_WARMUP": str(warmup).lower(),
    }
    if role == "hub":
        for name in ("billing", "booking"):
            environment[name.upper() + "_DOMAIN_AGENT"] = domains[name]["name"]
            environment[name.upper() + "_DOMAIN_VERSION"] = domains[name]["version"]
    return models.HostedAgentDefinition(
        cpu="1",
        memory="2Gi",
        enable_memory_snapshot=True,
        container_configuration=models.ContainerConfiguration(image=image),
        environment_variables=environment,
        protocol_versions=[models.ProtocolVersionRecord(protocol="invocations", version="2.0.0")],
        session_configuration=models.SessionConfiguration(idle_timeout_seconds=idle),
    )


def wait_active(client: AIProjectClient, name: str, version: str) -> Any:
    deadline = time.monotonic() + 600
    while True:
        resource = client.agents.get_version(agent_name=name, agent_version=version)
        status = resource.as_dict().get("status")
        print(f"{name}:{version} status={status}", flush=True)
        if status == "active":
            return resource
        if status == "failed" or time.monotonic() >= deadline:
            raise RuntimeError(f"Snapshot capture did not become active: {name}:{version}; inspect Foundry logs")
        time.sleep(5)


def create(
    client: AIProjectClient,
    endpoint: str,
    image: str,
    model: str,
    prefix: str,
    output: Path,
    *,
    idle: int = 120,
    warmup: bool = True,
) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,35}[a-z0-9]", prefix):
        raise ValueError("Use a 2–37 character lowercase alphanumeric/hyphen prefix")
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Use the immutable linux/amd64 runtime image@sha256:<64 hex digits>")
    if not model.strip() or not 120 <= idle <= 3600:
        raise ValueError("A model deployment and idle timeout 120–3600s are required")
    if output.exists():
        raise FileExistsError("Keep the existing deployment record; choose a fresh --output")
    run_id = uuid.uuid4().hex[:10]
    names = {role: f"{prefix}-{role}-{run_id}" for role in ("billing", "booking", "hub")}
    for name in names.values():
        try:
            client.agents.get(agent_name=name)
        except ResourceNotFoundError:
            continue
        raise RuntimeError("Refusing to add a version to an existing agent: " + name)
    result: dict[str, Any] = {
        "project_endpoint": project_endpoint(endpoint),
        "run_id": run_id,
        "image": image,
        "planned_names": names,
        "agents": {},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    for role, name in names.items():
        body = definition(endpoint, image, model, role, result["agents"], idle=idle, warmup=warmup)
        resource = client.agents.create_version(
            agent_name=name,
            definition=body,
            description="Snapshot-enabled Hub/Domain HTTP voice sample",
            metadata={
                "voiceLiveCompatible": "true",
                "voiceLiveInvocationState": "session_self_managed",
                "sample": "hub-domain-agent",
                "sampleRunId": run_id,
                "sampleRole": role,
            },
        )
        result["agents"][role] = {"name": name, "version": resource.version}
        output.write_text(json.dumps(result, indent=2) + "\n")
        live = wait_active(client, name, resource.version)
        observed = live.definition.as_dict()
        if any(observed.get(key) != value for key, value in body.as_dict().items()):
            raise RuntimeError("Created definition differs from requested snapshot/image/configuration")
        result["agents"][role]["instance_identity"] = live.as_dict().get("instance_identity")
        output.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=os.getenv("AGENT_IMAGE"))
    parser.add_argument("--prefix", default=os.getenv("AGENT_PREFIX", "hub-domain-demo"))
    parser.add_argument("--output", type=Path, default=Path("deployment.json"))
    args = parser.parse_args()
    endpoint = project_endpoint(os.environ["PROJECT_ENDPOINT"])
    with (
        DefaultAzureCredential() as credential,
        AIProjectClient(
            endpoint=endpoint,
            credential=credential,
            allow_preview=True,
            retry_total=0,
        ) as client,
    ):
        try:
            result = create(
                client,
                endpoint,
                args.image or "",
                os.environ["MODEL_DEPLOYMENT"],
                args.prefix,
                args.output,
                idle=int(os.getenv("IDLE_TIMEOUT_SECONDS", "120")),
                warmup=os.getenv("DOMAIN_MODEL_WARMUP", "true").lower() == "true",
            )
        except Exception:
            print(f"Creation failed. Keep {args.output}; inspect planned/accepted names before cleanup. No retries.")
            raise
    print(json.dumps(result, indent=2))
    print("Grant/verify model permissions for all identities and Domain session/invocation access for the Hub.")
    print("ACTIVE confirms capture, not business readiness. Continue with TESTING.md.")


if __name__ == "__main__":
    main()
