# Nasuni cluster access

Kind cluster on `nas-vm`.

> **Snapshot:** Inspected on 3 November 2026 at 12:34 UTC. Cluster manager
> reported a healthy cluster with three Ready nodes; the appliance and volume
> were Ready. Addresses, exports and status can change. Recheck before use.

## Cluster details

| Item | Value |
| --- | --- |
| Host / user | `nas-vm` / `automation` |
| Cluster / backend | `nasuni-cluster2` / Kind; three nodes |
| Kubernetes context | `kind-nasuni-cluster2` |
| Protocol VIP | `10.89.0.202`; NFS TCP 2049, SMB TCP 445 |
| Volume | `nasuni-cluster2-vol1` — Ready, Active, Healthy |
| Deploy checkout | `/home/automation/strider-historical-nasuni-cluster2/deploy-oct20` |
| Deploy commit | `9eca2d7039151df271f2853e75e6b28340bbe7a2` |
| Cluster-manager commit | `217bd9118c082497c0311637e50ea08b6098dd34` (deployment record; not reverified during this inspection) |

The published Chimera image's source commit has not been verified. The deploy
checkout and cluster-manager revisions do not establish which Chimera source
revision is running.

## Connect and select the cluster

From your workstation, connect to the host. Run the cluster commands on
`nas-vm`, not on your workstation.

```sh
ssh nas-vm
export STRIDER_DEPLOY_PATH=/home/automation/strider-historical-nasuni-cluster2/deploy-oct20
export PATH="/home/automation/strider-deploy/.venv/bin:$PATH"
export KUBECONFIG="$STRIDER_DEPLOY_PATH/strider-clusters/nasuni-cluster2/kind-kubeconfig"
kubectl config current-context
kubectl get --raw=/readyz
kubectl get nodes
```

The expected context is `kind-nasuni-cluster2`; `/readyz` returned `ok` at
inspection. Explicitly set `KUBECONFIG`: the host's default may point to a
different cluster. The kubeconfig contains credentials; keep it private and
do not paste it into chat. The protocol VIP is not the Kubernetes API.

If the isolated kubeconfig is unavailable, request a new one on `nas-vm`
using the command below and set `KUBECONFIG` to the path printed. This
command creates a temporary kubeconfig and was not re-run during the
inspection.

```sh
cluster-manager kubeconfig --backend kind \
  --cluster-name nasuni-cluster2 \
  --strider-deploy-path "$STRIDER_DEPLOY_PATH"
```

## Check health and volume

```sh
cluster-manager status --backend kind \
  --cluster-name nasuni-cluster2 \
  --strider-deploy-path "$STRIDER_DEPLOY_PATH"
kubectl -n nasuni get appliance-config
kubectl -n nasuni get volumes,nfs-exports,smb-shares
```

At inspection, cluster-manager reported healthy, the appliance was Ready and
`nasuni-cluster2-vol1` was Ready, Active and Healthy. These observations are
not a guarantee of current health.

## NFS and SMB access

The Chimera load balancer advertised `10.89.0.202` with NFS TCP 2049 and
SMB TCP 445. Client reachability was not rechecked during this inspection.
A Ready volume does **not** automatically create a mountable export or share.
Inspect the resources and verify an authorized export or share through the
supported Strider workflow before attempting a mount.

At inspection, NFS exports `test-ro` and `test-rw` were Ready, both mapped
to volume path `/test`; the former was read-only and the latter read-write.
Their client reachability and mount behavior were not tested in this
inspection. No SMB share resources were present.

During deployment, a disposable NFSv4.1 export named `demo` mapped the
volume root `/` and was mounted on `nas-vm` at `10.89.0.202:/demo`. A bounded
64 KiB write, fsync, read and delete passed. The `demo` export was removed
afterward; that historical path is not a current mount instruction. This
check does not establish access to other exports or SMB.

## Grafana

At inspection, `otel-lgtm-nodeport` in namespace `o11y` exposed port 3000
on node port 30300. Its address, login, dashboards and workstation
reachability were not verified for this cluster.

```sh
kubectl -n o11y get svc otel-lgtm-nodeport
kubectl -n o11y get pods
```

Do not expose Grafana or copy kubeconfig credentials to another machine
without an approved secure-access setup.
