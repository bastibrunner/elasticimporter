# elastic-api-importer

Helm chart for reconciling Elastic/Kibana API objects after an ECK deployment.

It renders payloads into labeled ConfigMaps, mounts them into a post-install/post-upgrade Job, runs a Python importer from a ConfigMap, and stores reconciliation state in a Kubernetes Secret named `applied-configuration` by default.

## Supported resource kinds

| Kind | API | Resource |
|---|---|---|
| `space` | Kibana | `/api/spaces/space` |
| `data-view` | Kibana | `/api/data_views/data_view/{id}` |
| `dashboard` | Kibana | `/api/dashboards/{id}` |
| `saved-search` | Kibana | saved object type `search` |
| `ml-job` | Elasticsearch | `/_ml/anomaly_detectors/{job_id}` |

Aliases accepted by the script: `data_view`, `index-pattern`, `index_pattern`, `saved_search`, `search`, `ml_job`, `machine-learning-job`.

## State model

The state Secret stores one JSON document in `data.state.json`:

```json
{
  "schemaVersion": 1,
  "managedBy": "elastic-api-importer",
  "resources": {
    "kibana:dashboard:space=observability:id=logs-overview": {
      "kind": "dashboard",
      "api": "kibana",
      "id": "logs-overview",
      "space": "observability",
      "payloadHash": "sha256:...",
      "apiHash": "sha256:..."
    }
  }
}
```

Reconciliation logic:

1. Load desired resources from mounted ConfigMaps.
2. Query Kibana/Elasticsearch for every desired object.
3. Create when missing.
4. Update when the payload hash changed.
5. Update when the current normalized API response hash differs from the last applied API hash.
6. Delete previously applied resources that disappeared from the Helm chart, unless `importer.deleteRemoved=false`, `gitops.disableDelete=true`, or the resource entry has `delete=false`.

The API response hash is calculated after a create/update follow-up GET. That captures Elastic/Kibana-generated defaults, so the next run does not reapply merely because the API returned fields that were not present in the original manifest. Handler-specific normalization ignores volatile Kibana metadata such as saved-object `version`/`updated_at` fields and dashboard panel `uid` values assigned by the API.

## Important limitations

Kubernetes cannot natively mount "all ConfigMaps matching a label" into a Pod. This chart therefore renders the payload ConfigMaps from `.Values.apiImports` and mounts exactly those ConfigMaps. They are still labeled with `itd.nrw.de/api-import: <kind>` by default, so the label remains the contract for ownership and inspection.

Data views use the Kibana Data Views API (`/api/data_views/data_view`). Payloads can be flat data view fields (`title`, `name`, `timeFieldName`, ...) or wrapped in a `data_view` object. Optional top-level keys `override` (create) and `refresh_fields` (update) are passed through to the API.

Dashboards use the Kibana Dashboards API (`/api/dashboards`). Saved searches still use the legacy saved-object API because Elastic has not published a replacement for that object type yet. Dashboard payloads should follow the Dashboards API schema (`title`, `panels`, `options`, ...). The importer still accepts legacy saved-object `attributes` payloads for dashboards and converts simple empty-dashboard definitions automatically.

ML job updates are restricted by Elasticsearch. The script calls `POST /_ml/anomaly_detectors/{job_id}/_update`; immutable changes will fail unless the resource has `replaceOnUpdate: true`, in which case the script deletes and recreates the ML job. That deletes model state/results.

## Credentials

Create a Secret with either API keys or basic-auth credentials. API-specific keys are preferred; fallback keys are used for both APIs.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: elastic-api-importer-credentials
type: Opaque
stringData:
  kibanaUsername: elastic
  kibanaPassword: changeme
  elasticsearchUsername: elastic
  elasticsearchPassword: changeme
  # Or:
  # kibanaApiKey: "<base64-api-key>"
  # elasticsearchApiKey: "<base64-api-key>"
```

## Example install

```bash
helm upgrade --install elastic-api-importer ./elastic-api-importer \
  --namespace elastic-system \
  -f examples/values-example.yaml
```

## Adding a resource

```yaml
apiImports:
  - name: observability-space
    kind: space
    id: observability
    payload: |-
      {
        "id": "observability",
        "name": "Observability",
        "description": "Managed by Helm"
      }

  - name: my-dashboard
    kind: dashboard
    id: "my-dashboard-id"
    space: observability
    payload: |-
      {
        "title": "My dashboard",
        "description": "Managed by Helm",
        "panels": [],
        "options": {
          "use_margins": true,
          "sync_colors": false,
          "sync_cursor": true,
          "sync_tooltips": false
        },
        "query": {
          "language": "kuery",
          "query": ""
        }
      }
```

## Extending

Add a new handler class in `scripts/importer.py`, register it in `HANDLER_TYPES`, then add payloads with a new `kind` value. The handler only needs to implement:

- `resolve_id()` if the ID is not `manifest.id`
- `get_current()`
- `create()`
- `update()`
- `delete()`
- optionally `normalize_api_response()`

The Helm templates do not need to change for new kinds unless you want different labels, mounts, or values schema.
