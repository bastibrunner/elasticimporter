#!/usr/bin/env python3
"""GitOps-style Elastic/Kibana API importer for Helm hook Jobs.

The script intentionally has no third-party dependencies. It reads payloads from
mounted ConfigMaps, reconciles them through Kibana/Elasticsearch APIs, and stores
minimal state in a Kubernetes Secret.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import logging
import os
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import error, parse, request

LOG = logging.getLogger("elastic-api-importer")

STATE_SCHEMA_VERSION = 1
STATE_DATA_KEY = "state.json"
DEFAULT_STATE = {"schemaVersion": STATE_SCHEMA_VERSION, "managedBy": "elastic-api-importer", "resources": {}}


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def deep_copy_json(value: Any) -> Any:
    return json.loads(json.dumps(value))


def without_empty(value: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in value.items() if v is not None and v != ""}


def quote_path(value: str) -> str:
    return parse.quote(value, safe="")


class ApiError(RuntimeError):
    def __init__(self, status: int, method: str, url: str, body: str):
        self.status = status
        self.method = method
        self.url = url
        self.body = body
        super().__init__(f"{method} {url} failed with HTTP {status}: {body[:1000]}")


class HttpClient:
    def __init__(
        self,
        name: str,
        base_url: str,
        api_key: str = "",
        username: str = "",
        password: str = "",
        ca_file: str = "",
        verify_tls: bool = True,
        timeout: int = 60,
        extra_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        if not base_url:
            raise ValueError(f"{name} base URL is not configured")
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.extra_headers = dict(extra_headers or {})
        self.ssl_context = self._ssl_context(ca_file, verify_tls)
        self.auth_headers = self._auth_headers(api_key, username, password)

    @staticmethod
    def _ssl_context(ca_file: str, verify_tls: bool) -> ssl.SSLContext:
        if not verify_tls:
            return ssl._create_unverified_context()  # nosec: explicit user setting
        if ca_file:
            return ssl.create_default_context(cafile=ca_file)
        return ssl.create_default_context()

    @staticmethod
    def _auth_headers(api_key: str, username: str, password: str) -> Dict[str, str]:
        if api_key:
            lower = api_key.lower()
            if lower.startswith("apikey ") or lower.startswith("bearer ") or lower.startswith("basic "):
                return {"Authorization": api_key}
            return {"Authorization": f"ApiKey {api_key}"}
        if username and password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            return {"Authorization": f"Basic {token}"}
        return {}

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        expected: Iterable[int] = (200, 201, 202, 204),
    ) -> Tuple[int, Any]:
        url = self.base_url + path
        all_headers = {"Accept": "application/json"}
        all_headers.update(self.auth_headers)
        all_headers.update(self.extra_headers)
        if headers:
            all_headers.update(headers)

        data: Optional[bytes]
        if body is None:
            data = None
        else:
            data = canonical_json_bytes(body)
            all_headers.setdefault("Content-Type", "application/json")

        req = request.Request(url=url, method=method, data=data, headers=all_headers)
        try:
            with request.urlopen(req, timeout=self.timeout, context=self.ssl_context) as response:
                status = response.status
                raw = response.read()
                return status, self._decode_response(raw, response.headers.get("Content-Type", ""))
        except error.HTTPError as exc:
            raw_text = exc.read().decode("utf-8", errors="replace")
            if exc.code in set(expected):
                return exc.code, self._decode_response(raw_text.encode("utf-8"), exc.headers.get("Content-Type", ""))
            raise ApiError(exc.code, method, url, raw_text) from exc
        except error.URLError as exc:
            raise RuntimeError(f"{method} {url} failed: {exc}") from exc

    @staticmethod
    def _decode_response(raw: bytes, content_type: str) -> Any:
        if not raw:
            return None
        text = raw.decode("utf-8", errors="replace")
        if "json" in content_type.lower() or text.strip().startswith(("{", "[")):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        return text

    def get(self, path: str, expected: Iterable[int] = (200,)) -> Tuple[int, Any]:
        return self.request("GET", path, expected=expected)

    def post(self, path: str, body: Any = None, expected: Iterable[int] = (200, 201, 202)) -> Tuple[int, Any]:
        return self.request("POST", path, body=body, expected=expected)

    def put(self, path: str, body: Any = None, expected: Iterable[int] = (200, 201, 202)) -> Tuple[int, Any]:
        return self.request("PUT", path, body=body, expected=expected)

    def delete(self, path: str, expected: Iterable[int] = (200, 202, 204, 404)) -> Tuple[int, Any]:
        return self.request("DELETE", path, expected=expected)


class KubernetesSecretStore:
    def __init__(self, namespace: str, secret_name: str, dry_run: bool = False) -> None:
        self.namespace = namespace
        self.secret_name = secret_name
        self.dry_run = dry_run
        token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        self.token = token_path.read_text(encoding="utf-8")
        self.client = HttpClient(
            name="kubernetes",
            base_url="https://kubernetes.default.svc",
            api_key=f"Bearer {self.token}",
            ca_file=str(ca_path),
            verify_tls=True,
            timeout=30,
        )

    @property
    def _path(self) -> str:
        return f"/api/v1/namespaces/{quote_path(self.namespace)}/secrets/{quote_path(self.secret_name)}"

    def load(self) -> Dict[str, Any]:
        status, response = self.client.get(self._path, expected=(200, 404))
        if status == 404:
            LOG.info("State Secret %s/%s does not exist yet", self.namespace, self.secret_name)
            return deep_copy_json(DEFAULT_STATE)

        encoded = (response.get("data") or {}).get(STATE_DATA_KEY)
        if not encoded:
            LOG.warning("State Secret exists but does not contain %s; starting with empty state", STATE_DATA_KEY)
            return deep_copy_json(DEFAULT_STATE)

        try:
            state = json.loads(base64.b64decode(encoded).decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - fail closed with a useful message
            raise RuntimeError(f"Could not decode state Secret {self.secret_name}/{STATE_DATA_KEY}: {exc}") from exc

        state.setdefault("schemaVersion", STATE_SCHEMA_VERSION)
        state.setdefault("managedBy", "elastic-api-importer")
        state.setdefault("resources", {})
        return state

    def save(self, state: Dict[str, Any]) -> None:
        state["schemaVersion"] = STATE_SCHEMA_VERSION
        state["managedBy"] = "elastic-api-importer"
        state["updatedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        encoded = base64.b64encode(canonical_json_bytes(state)).decode("ascii")

        if self.dry_run:
            LOG.info("DRY_RUN=true; not writing state Secret %s/%s", self.namespace, self.secret_name)
            return

        status, _ = self.client.get(self._path, expected=(200, 404))
        if status == 404:
            body = {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": self.secret_name,
                    "namespace": self.namespace,
                    "labels": {
                        "app.kubernetes.io/managed-by": "elastic-api-importer",
                    },
                },
                "type": "Opaque",
                "data": {STATE_DATA_KEY: encoded},
            }
            LOG.info("Creating state Secret %s/%s", self.namespace, self.secret_name)
            self.client.post(f"/api/v1/namespaces/{quote_path(self.namespace)}/secrets", body=body, expected=(200, 201))
            return

        patch_body = {
            "metadata": {"labels": {"app.kubernetes.io/managed-by": "elastic-api-importer"}},
            "data": {STATE_DATA_KEY: encoded},
        }
        LOG.info("Updating state Secret %s/%s", self.namespace, self.secret_name)
        self.client.request(
            "PATCH",
            self._path,
            body=patch_body,
            headers={"Content-Type": "application/merge-patch+json"},
            expected=(200,),
        )


@dataclass(frozen=True)
class DesiredResource:
    name: str
    kind: str
    resource_id: str
    space: str
    payload: Dict[str, Any]
    raw_payload: bytes
    delete: bool
    replace_on_update: bool
    config_map: str
    source_path: Path

    @property
    def payload_hash(self) -> str:
        return sha256_bytes(self.raw_payload)


class ResourceHandler:
    kind = ""
    api = ""

    def __init__(self, kibana: HttpClient, elasticsearch: HttpClient) -> None:
        self.kibana = kibana
        self.elasticsearch = elasticsearch

    def resolve_id(self, payload: Dict[str, Any], manifest: Dict[str, Any]) -> str:
        resource_id = str(manifest.get("id") or payload.get("id") or payload.get("job_id") or "").strip()
        if not resource_id:
            raise ValueError(f"Resource {manifest.get('name')} ({manifest.get('kind')}) has no id")
        return resource_id

    def state_key(self, resource: DesiredResource) -> str:
        parts = [self.api, self.kind]
        if resource.space:
            parts.append(f"space={resource.space}")
        parts.append(f"id={resource.resource_id}")
        return ":".join(parts)

    def get_current(self, resource: DesiredResource) -> Optional[Any]:
        raise NotImplementedError

    def create(self, resource: DesiredResource) -> None:
        raise NotImplementedError

    def update(self, resource: DesiredResource) -> None:
        raise NotImplementedError

    def delete(self, state_entry: Mapping[str, Any]) -> None:
        raise NotImplementedError

    def normalize_api_response(self, response: Any) -> Any:
        return response

    def api_hash(self, response: Any) -> str:
        return sha256_json(self.normalize_api_response(response))


class SpaceHandler(ResourceHandler):
    kind = "space"
    api = "kibana"

    def _path(self, space_id: str) -> str:
        return f"/api/spaces/space/{quote_path(space_id)}"

    def get_current(self, resource: DesiredResource) -> Optional[Any]:
        status, response = self.kibana.get(self._path(resource.resource_id), expected=(200, 404))
        return None if status == 404 else response

    def create(self, resource: DesiredResource) -> None:
        body = copy.deepcopy(resource.payload)
        body.setdefault("id", resource.resource_id)
        self.kibana.post("/api/spaces/space", body=body, expected=(200, 201))

    def update(self, resource: DesiredResource) -> None:
        body = copy.deepcopy(resource.payload)
        body["id"] = resource.resource_id
        self.kibana.put(self._path(resource.resource_id), body=body, expected=(200,))

    def delete(self, state_entry: Mapping[str, Any]) -> None:
        resource_id = str(state_entry["id"])
        if resource_id == "default":
            LOG.warning("Refusing to delete Kibana default space")
            return
        self.kibana.delete(self._path(resource_id), expected=(200, 204, 404))

    def normalize_api_response(self, response: Any) -> Any:
        if not isinstance(response, dict):
            return response
        normalized = copy.deepcopy(response)
        normalized.pop("_reserved", None)
        return normalized


class SavedObjectHandler(ResourceHandler):
    api = "kibana"
    object_type = ""

    def resolve_id(self, payload: Dict[str, Any], manifest: Dict[str, Any]) -> str:
        resource_id = str(manifest.get("id") or payload.get("id") or "").strip()
        if not resource_id:
            raise ValueError(f"Saved object {manifest.get('name')} requires manifest.id or payload.id")
        return resource_id

    @staticmethod
    def _space_prefix(space: str) -> str:
        if not space or space == "default":
            return ""
        return f"/s/{quote_path(space)}"

    def _path(self, resource_id: str, space: str = "") -> str:
        return f"{self._space_prefix(space)}/api/saved_objects/{quote_path(self.object_type)}/{quote_path(resource_id)}"

    def _body(self, resource: DesiredResource, *, create: bool) -> Dict[str, Any]:
        payload = copy.deepcopy(resource.payload)
        for key in (
            "id",
            "type",
            "namespaces",
            "originId",
            "updated_at",
            "created_at",
            "version",
            "migrationVersion",
            "coreMigrationVersion",
            "managed",
        ):
            payload.pop(key, None)
        if not create:
            payload.pop("initialNamespaces", None)
        if "attributes" not in payload:
            raise ValueError(f"Saved object {resource.name} payload must contain an attributes object")
        payload.setdefault("references", [])
        return payload

    def get_current(self, resource: DesiredResource) -> Optional[Any]:
        status, response = self.kibana.get(self._path(resource.resource_id, resource.space), expected=(200, 404))
        return None if status == 404 else response

    def create(self, resource: DesiredResource) -> None:
        self.kibana.post(self._path(resource.resource_id, resource.space), body=self._body(resource, create=True), expected=(200, 201))

    def update(self, resource: DesiredResource) -> None:
        self.kibana.put(self._path(resource.resource_id, resource.space), body=self._body(resource, create=False), expected=(200,))

    def delete(self, state_entry: Mapping[str, Any]) -> None:
        resource_id = str(state_entry["id"])
        space = str(state_entry.get("space") or "")
        path = self._path(resource_id, space)
        if env_bool("FORCE_DELETE_SAVED_OBJECTS", False):
            path += "?force=true"
        self.kibana.delete(path, expected=(200, 204, 404))

    def normalize_api_response(self, response: Any) -> Any:
        if not isinstance(response, dict):
            return response
        # Keep the object content, not Kibana's migration/version bookkeeping.
        return without_empty(
            {
                "id": response.get("id"),
                "type": response.get("type"),
                "attributes": response.get("attributes") or {},
                "references": response.get("references") or [],
            }
        )


class DashboardHandler(SavedObjectHandler):
    kind = "dashboard"
    object_type = "dashboard"


class SavedSearchHandler(SavedObjectHandler):
    kind = "saved-search"
    object_type = "search"


class DataViewHandler(ResourceHandler):
    kind = "data-view"
    api = "kibana"

    @staticmethod
    def _space_prefix(space: str) -> str:
        if not space or space == "default":
            return ""
        return f"/s/{quote_path(space)}"

    def _path(self, resource_id: str = "", space: str = "") -> str:
        base = f"{self._space_prefix(space)}/api/data_views/data_view"
        if resource_id:
            return f"{base}/{quote_path(resource_id)}"
        return base

    @staticmethod
    def _payload_parts(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, Optional[bool]]:
        body = copy.deepcopy(payload)
        override = bool(body.pop("override", False))
        refresh_fields = body.pop("refresh_fields", None)
        if isinstance(body.get("data_view"), dict):
            data_view = body.pop("data_view")
        else:
            data_view = body
        return data_view, override, refresh_fields

    def resolve_id(self, payload: Dict[str, Any], manifest: Dict[str, Any]) -> str:
        data_view, _, _ = self._payload_parts(payload)
        resource_id = str(manifest.get("id") or data_view.get("id") or "").strip()
        if not resource_id:
            raise ValueError(f"Data view {manifest.get('name')} requires manifest.id or payload.id")
        return resource_id

    def _request_body(self, resource: DesiredResource, *, create: bool) -> Dict[str, Any]:
        data_view, override, refresh_fields = self._payload_parts(resource.payload)
        if create:
            data_view.setdefault("id", resource.resource_id)
            body: Dict[str, Any] = {"data_view": data_view}
            if override:
                body["override"] = True
            return body

        data_view.pop("id", None)
        data_view.pop("namespaces", None)
        body = {"data_view": data_view}
        if refresh_fields is not None:
            body["refresh_fields"] = refresh_fields
        return body

    def get_current(self, resource: DesiredResource) -> Optional[Any]:
        status, response = self.kibana.get(self._path(resource.resource_id, resource.space), expected=(200, 404))
        return None if status == 404 else response

    def create(self, resource: DesiredResource) -> None:
        self.kibana.post(self._path(space=resource.space), body=self._request_body(resource, create=True), expected=(200,))

    def update(self, resource: DesiredResource) -> None:
        self.kibana.post(
            self._path(resource.resource_id, resource.space),
            body=self._request_body(resource, create=False),
            expected=(200,),
        )

    def delete(self, state_entry: Mapping[str, Any]) -> None:
        resource_id = str(state_entry["id"])
        space = str(state_entry.get("space") or "")
        self.kibana.delete(self._path(resource_id, space), expected=(204, 404))

    def normalize_api_response(self, response: Any) -> Any:
        if isinstance(response, dict) and isinstance(response.get("data_view"), dict):
            data_view = copy.deepcopy(response["data_view"])
        elif isinstance(response, dict):
            data_view = copy.deepcopy(response)
        else:
            return response
        # Field lists are refreshed from Elasticsearch and should not drive reconciliation.
        data_view.pop("fields", None)
        return without_empty(data_view)


class MLJobHandler(ResourceHandler):
    kind = "ml-job"
    api = "elasticsearch"

    def resolve_id(self, payload: Dict[str, Any], manifest: Dict[str, Any]) -> str:
        resource_id = str(manifest.get("id") or payload.get("job_id") or payload.get("id") or "").strip()
        if not resource_id:
            raise ValueError(f"ML job {manifest.get('name')} requires manifest.id or payload.job_id")
        return resource_id

    def _path(self, job_id: str) -> str:
        return f"/_ml/anomaly_detectors/{quote_path(job_id)}"

    def _body(self, resource: DesiredResource) -> Dict[str, Any]:
        body = copy.deepcopy(resource.payload)
        # job_id is the path parameter for create/update. Sending it in the body can make older versions reject the request.
        body.pop("job_id", None)
        body.pop("id", None)
        return body

    def get_current(self, resource: DesiredResource) -> Optional[Any]:
        status, response = self.elasticsearch.get(self._path(resource.resource_id), expected=(200, 404))
        return None if status == 404 else response

    def create(self, resource: DesiredResource) -> None:
        self.elasticsearch.put(self._path(resource.resource_id), body=self._body(resource), expected=(200, 201))

    def update(self, resource: DesiredResource) -> None:
        # Elasticsearch ML jobs only allow a subset of fields to be updated. If you need immutable
        # fields to change, set replaceOnUpdate=true on the resource and the reconciler will recreate it.
        self.elasticsearch.post(f"{self._path(resource.resource_id)}/_update", body=self._body(resource), expected=(200,))

    def delete(self, state_entry: Mapping[str, Any]) -> None:
        resource_id = str(state_entry["id"])
        self.elasticsearch.delete(f"{self._path(resource_id)}?force=true&wait_for_completion=true", expected=(200, 202, 404))

    def normalize_api_response(self, response: Any) -> Any:
        if isinstance(response, dict) and isinstance(response.get("jobs"), list) and response["jobs"]:
            job = copy.deepcopy(response["jobs"][0])
        else:
            job = copy.deepcopy(response)
        if isinstance(job, dict):
            for key in (
                "job_version",
                "create_time",
                "finished_time",
                "deleting",
                "model_snapshot_id",
                "model_snapshot_min_version",
                "results_retention_days",
            ):
                job.pop(key, None)
        return job


HANDLER_TYPES = {
    "space": SpaceHandler,
    "data-view": DataViewHandler,
    "data_view": DataViewHandler,
    "index-pattern": DataViewHandler,
    "index_pattern": DataViewHandler,
    "dashboard": DashboardHandler,
    "saved-search": SavedSearchHandler,
    "saved_search": SavedSearchHandler,
    "search": SavedSearchHandler,
    "ml-job": MLJobHandler,
    "ml_job": MLJobHandler,
    "machine-learning-job": MLJobHandler,
}

RESOURCE_PRIORITY = {
    "space": 10,
    "data-view": 15,
    "dashboard": 20,
    "saved-search": 30,
    "ml-job": 40,
}


def ordered_desired_resources(
    desired: Dict[str, Tuple[DesiredResource, ResourceHandler]],
) -> List[Tuple[str, Tuple[DesiredResource, ResourceHandler]]]:
    """Return desired resources in dependency order.

    Spaces must exist before space-scoped Kibana saved objects. The
    deterministic key fallback keeps hook runs stable across executions.
    """
    return sorted(
        desired.items(),
        key=lambda item: (
            RESOURCE_PRIORITY.get(item[1][0].kind, 100),
            item[0],
        ),
    )

def load_payloads(payload_dir: Path, handlers: Mapping[str, ResourceHandler]) -> Dict[str, Tuple[DesiredResource, ResourceHandler]]:
    if not payload_dir.exists():
        raise RuntimeError(f"Payload directory {payload_dir} does not exist")

    resources: Dict[str, Tuple[DesiredResource, ResourceHandler]] = {}
    for directory in sorted(p for p in payload_dir.iterdir() if p.is_dir()):
        manifest_path = directory / "manifest.json"
        payload_path = directory / "payload.json"
        if not manifest_path.exists() or not payload_path.exists():
            LOG.warning("Skipping %s; manifest.json or payload.json missing", directory)
            continue
        LOG.info("Loading %s...", directory)

        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as e:
            LOG.error("Error loading %s: %s",manifest_path,e)
            continue
        try:
            raw_payload = payload_path.read_bytes()
            payload = json.loads(raw_payload.decode("utf-8"))
        except Exception as e:
            LOG.error("Error loading %s: %s",payload_path,e)
            continue

        if not isinstance(payload, dict):
            LOG.error("Payload %s could not be parsed as dict. Skipping.", payload_path)
            continue

        kind = str(manifest.get("kind", "")).strip()
        if kind not in handlers:
            LOG.error("Unsupported import kind \"%s\" in %s", kind, payload_path)
            continue

        handler = handlers[kind]
        resource_id = handler.resolve_id(payload, manifest)
        resource = DesiredResource(
            name=str(manifest.get("name") or directory.name),
            kind=handler.kind,
            resource_id=resource_id,
            space=str(manifest.get("space") or ""),
            payload=payload,
            raw_payload=raw_payload,
            delete=bool(manifest.get("delete", True)),
            replace_on_update=bool(manifest.get("replaceOnUpdate", False)),
            config_map=str(manifest.get("configMap") or ""),
            source_path=directory,
        )
        key = handler.state_key(resource)
        if key in resources:
            LOG.error("Duplicate desired resource key \"%s\" in %s", key, payload_path)
            continue

        resources[key] = (resource, handler)

    LOG.info("Loaded %d desired resources from %s", len(resources), payload_dir)
    return resources


def state_entry(resource: DesiredResource, handler: ResourceHandler, api_hash: str) -> Dict[str, Any]:
    return without_empty(
        {
            "kind": handler.kind,
            "api": handler.api,
            "id": resource.resource_id,
            "space": resource.space,
            "name": resource.name,
            "configMap": resource.config_map,
            "delete": resource.delete,
            "replaceOnUpdate": resource.replace_on_update,
            "payloadHash": resource.payload_hash,
            "apiHash": api_hash,
            "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )


def reconcile_resource(
    key: str,
    resource: DesiredResource,
    handler: ResourceHandler,
    state_resources: MutableMapping[str, Any],
    dry_run: bool,
) -> None:
    LOG.info("Reconciling %s (%s/%s)", key, resource.config_map or resource.source_path.name, resource.name)
    current = handler.get_current(resource)
    prior = state_resources.get(key)

    action = "noop"
    reason = "state and API hash match"

    if current is None:
        action = "create"
        reason = "resource does not exist"
    else:
        current_hash = handler.api_hash(current)
        if prior is None:
            action = "update"
            reason = "resource exists but is not in importer state"
        elif prior.get("payloadHash") != resource.payload_hash:
            action = "update"
            reason = "desired payload hash changed"
        elif prior.get("apiHash") != current_hash:
            action = "update"
            reason = "current API hash differs from last applied API hash"

    if action == "noop":
        LOG.info("No change for %s", key)
        return

    LOG.info("%s %s: %s", action.upper(), key, reason)
    if dry_run:
        LOG.info("DRY_RUN=true; not applying %s for %s", action, key)
        return

    if action == "create":
        handler.create(resource)
    else:
        try:
            handler.update(resource)
        except ApiError as exc:
            if handler.kind == "ml-job" and resource.replace_on_update:
                LOG.warning("ML job update failed; replaceOnUpdate=true, recreating %s. Error was: %s", key, exc)
                handler.delete(state_entry(resource, handler, ""))
                handler.create(resource)
            else:
                raise

    after = handler.get_current(resource)
    if after is None:
        raise RuntimeError(f"{action} for {key} completed but follow-up GET returned 404")
    state_resources[key] = state_entry(resource, handler, handler.api_hash(after))


def delete_removed(
    desired_keys: set[str],
    state_resources: MutableMapping[str, Any],
    handlers_by_kind: Mapping[str, ResourceHandler],
    dry_run: bool,
) -> None:
    for key in sorted(list(state_resources.keys())):
        if key in desired_keys:
            continue
        entry = state_resources[key]
        if not entry.get("delete", True):
            LOG.info("Keeping %s because its last state entry had delete=false", key)
            continue
        kind = str(entry.get("kind") or "")
        handler = handlers_by_kind.get(kind)
        if handler is None:
            LOG.warning("Cannot delete %s; no handler registered for kind %r", key, kind)
            continue
        LOG.info("DELETE %s: no longer present in desired payloads", key)
        if dry_run:
            LOG.info("DRY_RUN=true; not deleting %s", key)
            continue
        handler.delete(entry)
        state_resources.pop(key, None)


def build_clients() -> Tuple[HttpClient, HttpClient]:
    verify_tls = env_bool("VERIFY_TLS", True)
    timeout = env_int("REQUEST_TIMEOUT_SECONDS", 60)

    fallback_api_key = os.getenv("API_KEY", "")
    fallback_username = os.getenv("API_USERNAME", "")
    fallback_password = os.getenv("API_PASSWORD", "")

    kibana = HttpClient(
        name="kibana",
        base_url=os.getenv("KIBANA_URL", ""),
        api_key=os.getenv("KIBANA_API_KEY", "") or fallback_api_key,
        username=os.getenv("KIBANA_USERNAME", "") or fallback_username,
        password=os.getenv("KIBANA_PASSWORD", "") or fallback_password,
        ca_file=os.getenv("KIBANA_CA_FILE", "") if "https" in os.getenv("KIBANA_URL", "").lower() else "",
        verify_tls=verify_tls,
        timeout=timeout,
        extra_headers={"kbn-xsrf": "elastic-api-importer"},
    )
    elasticsearch = HttpClient(
        name="elasticsearch",
        base_url=os.getenv("ELASTICSEARCH_URL", ""),
        api_key=os.getenv("ELASTICSEARCH_API_KEY", "") or fallback_api_key,
        username=os.getenv("ELASTICSEARCH_USERNAME", "") or fallback_username,
        password=os.getenv("ELASTICSEARCH_PASSWORD", "") or fallback_password,
        ca_file=os.getenv("ELASTICSEARCH_CA_FILE", "") if "https" in os.getenv("ELASTICSEARCH_URL", "").lower() else "",
        verify_tls=verify_tls,
        timeout=timeout,
    )
    return kibana, elasticsearch


def main() -> int:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dry_run = env_bool("DRY_RUN", False)
    delete_removed_enabled = env_bool("DELETE_REMOVED", True)
    payload_dir = Path(os.getenv("PAYLOAD_DIR", "/payloads"))
    namespace = os.getenv("POD_NAMESPACE") or Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read_text(encoding="utf-8")
    state_secret_name = os.getenv("STATE_SECRET_NAME", "applied-configuration")

    kibana, elasticsearch = build_clients()
    handlers: Dict[str, ResourceHandler] = {}
    handlers_by_kind: Dict[str, ResourceHandler] = {}
    for alias, handler_type in HANDLER_TYPES.items():
        handler = handlers_by_kind.get(handler_type.kind)
        if handler is None:
            handler = handler_type(kibana, elasticsearch)
            handlers_by_kind[handler.kind] = handler
        handlers[alias] = handler
        handlers[handler.kind] = handler

    desired = load_payloads(payload_dir, handlers)

    store = KubernetesSecretStore(namespace=namespace, secret_name=state_secret_name, dry_run=dry_run)
    state = store.load()
    state_resources = state.setdefault("resources", {})

    for key, (resource, handler) in ordered_desired_resources(desired):
        reconcile_resource(key, resource, handler, state_resources, dry_run=dry_run)

    if delete_removed_enabled:
        delete_removed(set(desired.keys()), state_resources, handlers_by_kind, dry_run=dry_run)
    else:
        LOG.info("DELETE_REMOVED=false; not deleting resources missing from desired payloads")

    store.save(state)
    LOG.info("Reconciliation finished: %d desired resources, %d resources in state", len(desired), len(state_resources))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - top-level error logging
        LOG.exception("Importer failed: %s", exc)
        raise SystemExit(1) from exc
