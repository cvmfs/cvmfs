A Helm chart for the CVMFS-CSI driver, allowing the mounting of CVMFS repositories in Kubernetes environments. This chart will deploy the CSI driver as a DaemonSet, thus automatically scaling the driver on each cluster node.

## Usage

### Install from CERN repositories

You can install the Helm chart from CERN repositories:

Helm installation:
```
helm install cvmfs-csi oci://registry.cern.ch/kubernetes/charts/cvmfs-csi --version <Chart tag>
```

### Install from source

To use this Helm chart, clone this repository and install the chart:
```
git clone https://github.com/cvmfs/csi
```

Helm v2 installation:
```
helm install --name cvmfs ./cvmfs-csi/deployments/helm/cvmfs-csi
```

Helm v3 installation:
```
helm install cvmfs ./cvmfs-csi/deployments/helm/cvmfs-csi
```

## Configuration

The following table lists the configurable parameters of the CVMFS-CSI chart.

One can specify each parameter using the `--set key=value[,key=value]` argument to `helm install`.

Alternatively, a YAML file that specifies the values of the parameters can be provided when installing the chart via `-f /path/to/myvalues.yaml`.


| Parameter                                    | Description                                                                                                            |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `cvmfsConfig."default.local".configMapName` | Name of the ConfigMap (to use or create) for /etc/cvmfs/default.local file.                                             |
| `cvmfsConfig."default.local".use` | Whether to use this ConfigMap in /etc/cvmfs/default.local.                                                                        |
| `cvmfsConfig."default.local".create` | Whether to create default.local ConfigMap. If not, and `use` is set to true, it is expected the ConfigMap is already present.  |
| `cvmfsConfig."default.local".data` | default.local ConfigMap contents to use when `create` is set to true.                                                            |
| `cvmfsConfig."config.d".configMapName` | Name of the ConfigMap (to use or create) for /etc/cvmfs/config.d directory.                                                  |
| `cvmfsConfig."config.d".use` | Whether to use this ConfigMap in /etc/cvmfs/config.d.                                                                                  |
| `cvmfsConfig."config.d".create` | Whether to create config.d ConfigMap. If not, and `use` is set to true, it is expected the ConfigMap is already present.            |
| `cvmfsConfig."config.d".data` | config.d ConfigMap contents to use when `create` is set to true.                                                                      |
| `cache.local.volumeSpec` | Volume spec for local cache. ReadWriteOnce access mode for persistent volumes is sufficient.                                               |
| `cache.local.cvmfsQuotaLimit` | Maximum size of local cache in MiB. CVMFS client will garbage collect the exceeding amount.                                           |
| `cache.alien.enabled` | Whether to use alien cache in deployment.                                                                                                     |
| `cache.alien.volumeSpec` | Volume spec for local cache. ReadWriteMany access mode for persistent volumes is required.                                                 |
| `nodeplugin.name` | Component name for node plugin component. Used as `component` label value and to generate DaemonSet name.                                         |
| `nodeplugin.plugin.image.repository` | Container image repository for CVMFS CSI node plugin.                                                                          |
| `nodeplugin.plugin.image.tag` | Container image tag for CVMFS CSI node plugin.                                                                                        |
| `nodeplugin.plugin.image.pullPolicy` | Pull policy for CVMFS CSI node plugin image.                                                                                   |
| `nodeplugin.plugin.image.resources` | Resource constraints for the `nodeplugin` container.                                                                            |
| `nodeplugin.automount.image.repository` | Container image repository for CVMFS CSI node plugin (automount-runner).                                                    |
| `nodeplugin.automount.image.tag` | Container image tag for CVMFS CSI node plugin (automount-runner).                                                                  |
| `nodeplugin.automount.image.pullPolicy` | Pull policy for CVMFS CSI node plugin image (automount-runner).                                                             |
| `nodeplugin.automount.image.resources` | Resource constraints for the `automount` container.                                                                          |
| `nodeplugin.singlemount.image.repository` | Container image repository for CVMFS CSI node plugin (singlemount-runner).                                                |
| `nodeplugin.singlemount.image.tag` | Container image tag for CVMFS CSI node plugin (singlemount-runner).                                                              |
| `nodeplugin.singlemount.image.pullPolicy` | Pull policy for CVMFS CSI node plugin image (singlemount-runner).                                                         |
| `nodeplugin.singlemount.image.resources` | Resource constraints for the `singlemount` container.                                                                      |
| `nodeplugin.registrar.image.repository` | Container image repository for csi-node-driver-registrar.                                                                   |
| `nodeplugin.registrar.image.tag` | Container image tag for csi-node-driver-registrar.                                                                                 |
| `nodeplugin.registrar.image.pullPolicy` | Pull policy for csi-node-driver-registrar image.                                                                            |
| `nodeplugin.registrar.image.resources` | Resource constraints for the `registrar` container.                                                                          |
| `nodeplugin.registrar.image.repository` | Container image repository for csi-node-driver-registrar.                                                                   |
| `nodeplugin.registrar.image.tag` | Container image tag for csi-node-driver-registrar.                                                                                 |
| `nodeplugin.registrar.image.pullPolicy` | Pull policy for csi-node-driver-registrar image.                                                                            |
| `nodeplugin.registrar.image.resources` | Resource constraints for the `registrar` container.                                                                          |
| `nodeplugin.updateStrategySpec` | DaemonSet update strategy.                                                                                                          |
| `nodeplugin.priorityClassName` | Pod priority class name of the nodeplugin DaemonSet.                                                                                 |
| `nodeplugin.nodeSelector` | Pod node selector of the nodeplugin DaemonSet.                                                                                            |
| `nodeplugin.tolerations` | Pod tolerations of the nodeplugin DaemonSet.                                                                                               |
| `nodeplugin.affinity` | Pod node affinity of the nodeplugin DaemonSet.                                                                                                |
| `controllerplugin.name` | Component name for controller plugin component. Used as `component` label value and to generate Deployment name.                            |
| `controllerplugin.plugin.image.repository` | Container image repository for CVMFS CSI controller plugin.                                                              |
| `controllerplugin.plugin.image.tag` | Container image tag for CVMFS CSI controller plugin.                                                                            |
| `controllerplugin.plugin.image.pullPolicy` | Pull policy for CVMFS CSI controller plugin image.                                                                       |
| `controllerplugin.plugin.image.resources` | Resource constraints for the `controllerplugin` container.                                                                |
| `controllerplugin.provisioner.image.repository` | Container image repository for external-provisioner.                                                                |
| `controllerplugin.provisioner.image.tag` | Container image tag for external-provisioner.                                                                              |
| `controllerplugin.provisioner.image.pullPolicy` | Pull policy for external-provisioner image.                                                                         |
| `controllerplugin.provisioner.image.resources` | Resource constraints for the `provisioner` container.                                                                |
| `controllerplugin.updateStrategySpec` | Deployment update strategy.                                                                                                   |
| `controllerplugin.priorityClassName` | Pod priority class name of the controllerplugin Deployment.                                                                    |
| `controllerplugin.nodeSelector` | Pod node selector of the controllerplugin Deployment.                                                                               |
| `controllerplugin.tolerations` | Pod tolerations of the controllerplugin Deployment.                                                                                  |
| `controllerplugin.affinity` | Pod node affinity of the controllerplugin Deployment.                                                                                   |
| `logVerbosityLevel` | Log verbosity of all containers.                                                                                                                |
| `csiDriverName` | CVMFS CSI driver name used as driver identifier by Kubernetes.                                                                                      |
| `kubeletDirectory` | Kubelet's plugin directory path.                                                                                                                 |
| `cvmfsCSIPluginSocketFile` | Name of the CVMFS CSI socket file.                                                                                                       |
| `startAutomountDaemon` | Whether CVMFS CSI nodeplugin Pod should run automount daemon.                                                                                |
| `automountHostPath` | Path on the host where to mount the autofs-managed CVMFS root. The directory will be created if it doesn't exist.                               |
| `automountStorageClass.create` | Whether a CVMFS CSI storage class using the automounter should be created automatically.                                             |
| `automountStorageClass.name` | The name for the CVMFS CSI storage class using the automounter if created.                                                             |
| `specificRepositoryStorageClasses` | A list of specific CVMFS repos you wish to generate a `storageClass` for.                                                        |
| `nameOverride` | Chart name override.                                                                                                                                 |
| `fullNameOverride` | Chart name override.                                                                                                                             |
| `extraMetaLabels` | Extra Kubernetes object metadata labels to be added the ones generated with cvmfs-csi.common.metaLabels template.                                 |
