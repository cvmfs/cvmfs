# Upgrade procedure from CVMFS CSI v1 to v2 (Helm)

This guide shows one of the possible ways to upgrade Helm deployment of CVMFS CSI from v1 to v2.

Upgrading CVMFS CSI involves deleting Pods that are using CVMFS volumes. Please make sure your environment allows for this before proceeding.

## Procedure

It is assumed that the cvmfs-csi v2 Helm chart is available in `cern/csi-cvmfs` Helm repository. You can see [Deployment with Helm chart](./deploying.md#deployment-with-helm-chart) guide how to add it, or [deployments/helm/README.md](../deployments/helm/README.md) if installing from source.

1. Set `updateStrategy` of the Node plugin DaemonSet to `OnDelete`:
   `kubectl patch daemonset csi-cvmfsplugin -p '{"spec": {"updateStrategy": {"type": "OnDelete"}}}'`
2. Create `values-v1-v2-upgrade.yaml` values file:

   ```yaml
   nodeplugin:

     # Override DaemonSet name to be the same as the one used in v1 deployment.
     fullnameOverride: "csi-cvmfsplugin"

     # DaemonSet matchLabels must be the same too.
     matchLabelsOverride:
       app: csi-cvmfsplugin

     # Create a dummy ServiceAccount for compatibility with v1 DaemonSet.
     serviceAccount:
       create: true
       use: true
   ```
3. Upgrade cvmfs-csi 0.3.0 Helm chart to cvmfs-csi 2.0.0:
   `helm upgrade <CVMFS CSI release name> cern/cvmfs-csi --version 2.0.0 --values values-v1-v2-upgrade.yaml`
4. For every node where Node plugin Pod is running:

   1. Delete csi-cvmfsplugin v1 DaemonSet Pod on this node, and wait until the v2 Pod is running. Note that doing so will make CVMFS mounts of all application Pods on this node invalid and further inaccessible, as the CVMFS client process that served the mount exits.
   2. Delete all Pods on this node that are using CVMFS volumes. This refreshes the mounts and makes them available again, now with the CVMFS CSI v2.
