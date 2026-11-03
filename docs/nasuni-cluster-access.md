# Nasuni cluster access

Kind cluster on `nas-vm`.

> **Snapshot:** Inspected on 3 November 2026 at 05:53 UTC. The cluster was
> healthy with three Ready nodes, a Ready appliance, and a Ready volume.
> Addresses and status can change.

## Cluster details

| Item | Value |
| --- | --- |
| Host / user | `nas-vm` / `automation` |
| Cluster / backend | `nasuni-cluster` / Kind; three nodes |
| Kubernetes context | `kind-nasuni-cluster` |
| Kubernetes API | `https://127.0.0.1:44191` on `nas-vm` only |
| Protocol VIP | `10.89.0.200`; NFS TCP 2049, SMB TCP 445 |
| Azure-backed volume | `vol-azure-c1b4a8bb` — Ready |
| Volume GUID | `vol-azure-c1b4a8bb-guid` |

## Connect and select the cluster

From your workstation, connect to the host. The tools below are installed on
`nas-vm`; do not run the cluster commands on your workstation.

```sh
ssh nas-vm
export STRIDER_DEPLOY_PATH=/home/automation/strider-deploy
export PATH="$STRIDER_DEPLOY_PATH/.venv/bin:$PATH"
```

The existing kubeconfig below was verified on `nas-vm`. It lives under `/tmp`
and may disappear on reboot or cleanup. It contains credentials: keep it private
and do not paste it into chat.

```sh
export KUBECONFIG=/tmp/kubeconfig-nasuni-cluster-1yx082ad.yaml
kubectl config current-context
kubectl get --raw=/readyz
kubectl get nodes
```

The expected context is `kind-nasuni-cluster` and `/readyz` returned `ok`.
Explicitly set `KUBECONFIG`: the host's default may point to a different
cluster. The API address is loopback on `nas-vm`, not the protocol VIP.

If the temporary kubeconfig no longer exists, request a new one on `nas-vm`
using the command below and set `KUBECONFIG` to the path printed. The command
is documented but was not re-run during the read-only inspection; it creates a
temporary kubeconfig.

```sh
cluster-manager kubeconfig --backend kind \
  --cluster-name nasuni-cluster \
  --strider-deploy-path "$STRIDER_DEPLOY_PATH"
```

## Check health and volume

```sh
cluster-manager status --backend kind \
  --cluster-name nasuni-cluster \
  --strider-deploy-path "$STRIDER_DEPLOY_PATH"
kubectl -n nasuni get appliance-config
kubectl -n nasuni get volumes,nfs-exports,smb-shares
```

The status command reported healthy during deployment. At the later inspection,
the API, all three nodes, appliance and volume were Ready. The `kubectl get`
resource-list commands above are inspection commands; their exact invocations
were not re-run in the later check.

## NFS and SMB access

The protocol service exposes `10.89.0.200` on the Kind Podman network. The route
was verified from `nas-vm`; reachability from other hosts was not verified. There
were no NFS export or SMB share resources at inspection, so the Ready volume is
not yet a confirmed mountable share. Create and verify an export or share through
the supported Strider workflow before attempting a mount. Do not treat the VIP
as the Kubernetes API.

## Grafana

Grafana was running in namespace `o11y`. From `nas-vm`, the login page returned
HTTP 200 at `http://10.89.0.7:30300/login`. This is a Kind node address and may
change; `127.0.0.1:30300` was not reachable. Authentication, dashboards, and
workstation access were not verified.

```sh
kubectl -n o11y get svc grafana-nodeport
kubectl -n o11y get pods
```

Do not expose Grafana or copy kubeconfig credentials to another machine without
an approved secure-access setup.
