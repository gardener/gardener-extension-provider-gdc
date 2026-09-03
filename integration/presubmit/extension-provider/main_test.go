// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"

	"strings"
	"testing"
	"time"

	kubernetesclientset "k8s.io/client-go/kubernetes"

	"github.com/Masterminds/semver/v3"
	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	resourcesv1alpha1 "github.com/gardener/gardener/pkg/apis/resources/v1alpha1"
	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	vpaautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ipamglobalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/ipam/v1"
	globalnetworkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/networking/v1"
	globalv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/global/object/v1"
	ipamv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/ipam/v1"
	networkingv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/networking/v1"
	vmv1 "github.com/googlecloudplatform/google-distributed-cloud-apis/pkg/apis/public/virtualmachine/v1"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
	gdcclient "github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/client"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/gdc"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/gdcloud"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/helm"
	"github.com/gardener/gardener-extension-provider-gdc/integration/pkg/kubernetes"
	gdcinstall "github.com/gardener/gardener-extension-provider-gdc/pkg/apis/gdc/install"
)

var (
	commitHash          = flag.String("commit_hash", "", "the short commit hash for git repo")
	vclusterChart       = flag.String("vcluster_chart", "", "Path to vcluster helm chart")
	privateRegistry     = flag.String("private_registry", "", "Private cache registry to mirror vcluster images")
	zone                = flag.String("zone", "", "the zone where vuc is deployed to")
	region              = flag.String("region", "", "the region where vuc is deployed to")
	availableZones      = flag.String("available_zones", "", "the available zones for Multi-zone setup provided as comma-separated value")
	project             = flag.String("project", "", "the project where the vuc is created")
	vuc                 = flag.String("vuc", "", "the vuc name")
	org                 = flag.String("org", "", "the org name for gdc-ag lab")
	labURL              = flag.String("lab_url", "", "the lab url for gdc-ag lab, e.g. staging.gpcdemolabs.com")
	cafile              = flag.String("cafile", "", "the ca file to use for authentication")
	safile              = flag.String("service_account", "", "the service account to use for authentication")
	imagePullCredential = flag.String("image_pull_credential", "", "the config.json to use for image pull and push")
	imageTag            = flag.String("image_tag", "", "the image tag for extension-provider. In the format image:tag")
	chartPackage        = flag.String("chart_package", "", "the helm chart extension-provider to deploy")
	managedDNSZone      = flag.String("managed_dns_zone", "", "the existing managed dns zone in staging lab")
	vclusterK8sTag      = flag.String("vcluster_k8s_tag", "v1.35.0", "the kubernetes version tag for vcluster")
	controllers         = flag.String("controllers", "", "comma-separated list of controllers/webhooks to run. If empty, all are run.")
)

// k8sVersion returns the normalized semantic version without the leading 'v' (e.g. "1.35.0")
func k8sVersion() string {
	return strings.TrimPrefix(*vclusterK8sTag, "v")
}

// k8sSemVer returns the parsed semver.Version for the target cluster
func k8sSemVer() *semver.Version {
	return semver.MustParse(k8sVersion())
}

type commonTestFixture struct {
	namespace string
	gdcClient *gdcloud.TestingClient

	vucClient          client.WithWatch
	globalClient       client.WithWatch
	gdchConfig         *gdcclient.OrgClusterConfig
	mgmtClient         client.WithWatch
	scheme             *runtime.Scheme
	project            string
	vclusterKubeconfig string
}

// NewVClusterClient returns a new, isolated client connected to the vcluster.
func (f *commonTestFixture) NewVClusterClient(t *testing.T) client.WithWatch {
	t.Helper()
	restConfig, err := clientcmd.BuildConfigFromFlags("", f.vclusterKubeconfig)
	if err != nil {
		t.Fatalf("cannot build rest config from vcluster kubeconfig: %v", err)
	}
	restConfig.QPS = 100.0
	restConfig.Burst = 150

	c, err := client.NewWithWatch(restConfig, client.Options{Scheme: f.scheme})
	if err != nil {
		t.Fatalf("cannot create client for vcluster: %v", err)
	}
	return c
}

// TestExtensionProvider runs the presubmit tests for the extension provider.
func TestExtensionProvider(t *testing.T) {
	if *cafile == "" || *safile == "" {
		t.Skip("skipping integration test: -cafile or -service_account not provided")
	}
	if *commitHash == "" {
		t.Fatal("commit_hash flag is required")
	}
	t.Logf("Running extension-provider presubmit test for commit %q", *commitHash)

	common := setup(t, context.Background())

	// setup test fixure for each controller
	workerControllerTestFixture := workerControllerFixture{
		commonTestFixture: common,
		workerNamespace:   common.namespace + "-worker",
	}
	infraFixture := infraTestFixture{
		commonTestFixture: common,
		availableZones:    strings.Split(*availableZones, ","),
	}
	backupFixture := backupTestFixture{
		commonTestFixture: common,
	}
	runAll := len(*controllers) == 0
	selected := make(map[string]bool)
	if !runAll {
		parts := strings.Split(*controllers, ",")
		for _, p := range parts {
			name := strings.TrimSpace(p)
			selected[name] = true
		}
	}

	shouldRun := func(name string) bool {
		if runAll {
			return true
		}
		return selected[strings.TrimSpace(name)]
	}

	bastionFixture := &bastionTestFixture{
		commonTestFixture: common,
		bastionNamespace:  common.namespace + "-bastion",
	}
	controlPlaneFixture := controlPlaneTestFixture{
		commonTestFixture:     common,
		controlPlaneNamespace: common.namespace + "-controlplane",
		availableZones:        strings.Split(*availableZones, ","),
	}
	extensionProviderWebhookTestFixture := extensionProviderWebhookTestFixture{
		commonTestFixture: common,
	}

	controllersList := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "WorkerController",
			run:  workerControllerTestFixture.test,
		},
		{
			name: "BastionController",
			run:  bastionFixture.test,
		},
		{
			name: "InfraController",
			run:  infraFixture.test,
		},
		{
			name: "BackupController",
			run:  backupFixture.test,
		},
		{
			name: "ControlplaneController",
			run:  controlPlaneFixture.test,
		},
		{
			name: "DNSRecordController",
			run: func(t *testing.T) {
				t.Log("Initializing DNSRecord fixture...")
				dnsrecordFixture := setupDNSRecordFixture(t, common)
				dnsrecordFixture.test(t)
			},
		},
	}

	runControllersGroup := false
	for _, c := range controllersList {
		if shouldRun(c.name) {
			runControllersGroup = true
			break
		}
	}

	// Run controller tests in parallel, but wait for all to finish before cleaning up
	if runControllersGroup {
		t.Run("Controllers", func(t *testing.T) {
			for _, c := range controllersList {
				c := c // Capture range variable for closure
				if shouldRun(c.name) {
					t.Run(c.name, func(t *testing.T) {
						t.Parallel()
						c.run(t)
					})
				}
			}
		})
	}

	if shouldRun("ExtensionProviderWebhook") {
		t.Run("ExtensionProviderWebhook", func(t *testing.T) {
			t.Parallel()
			extensionProviderWebhookTestFixture.test(t)
		})
	}
}

// setup initializes the test environment, including clients, namespace, and helm chart.
func setup(t *testing.T, ctx context.Context) *commonTestFixture {

	// Initialize gdcloud
	consoleURL := "https://console." + *org + "." + *zone + "." + *labURL
	gdcClient, err := gdcloud.NewTestingClient(*cafile, *safile, consoleURL)
	if err != nil {
		t.Fatalf("unable to initialize gdcloud %v", err)
	}
	t.Cleanup(func() {
		if err := gdcClient.Cleanup(); err != nil {
			t.Errorf("failed to cleanup gdcloud client: %v", err)
		}
	})

	// Generate kubeconfig for user cluster
	hostKubeconfig, err := gdc.GetUserClusterKubeconfig(gdcClient, *zone, *project, *vuc)
	if err != nil {
		t.Fatalf("cannot generate kubeconfig for vuc %v", err)
	}

	// Create Test Fixture
	fixture := createTestFixture(t, ctx, gdcClient)

	// Inject image pull credential into the host namespace so the vcluster pod
	// can pull its images (e.g., loft-sh/kubernetes) from the private registry.
	setupImagePullSecret(t, ctx, fixture.vucClient, fixture.namespace)

	vclusterName := "vcluster-" + ptr.Deref(commitHash, "")
	vclusterClient, vcKubeconfigPath := setupVCluster(t, ctx, fixture.vucClient, fixture.namespace, vclusterName, fixture.scheme, hostKubeconfig)

	// We must recreate the test namespace inside the vcluster so tests can deploy to it
	if err := kubernetes.CreateNamespace(ctx, vclusterClient, fixture.namespace); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("cannot create namespace %q inside vcluster, %v", fixture.namespace, err)
		}
	}

	// We must recreate the "garden" namespace required by some tests (e.g. Backup, FluentBit).
	// It is initialized here in global setup to avoid parallel test conflicts; individual
	// test-level creations/cleanups would cause race conditions on the shared vcluster workspace.
	if err := kubernetes.CreateNamespace(ctx, vclusterClient, "garden"); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("cannot create namespace %q inside vcluster: %v", "garden", err)
		}
	}
	t.Cleanup(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "garden"}}
		if err := vclusterClient.Delete(ctx, ns); client.IgnoreNotFound(err) != nil {
			t.Errorf("failed to delete namespace %q inside vcluster: %v", "garden", err)
		}
	})

	installCRDs(t, ctx, vclusterClient, k8sSemVer())
	setupImagePullSecret(t, ctx, vclusterClient, fixture.namespace)
	releaseName := setupHelmChart(t, vcKubeconfigPath, fixture.namespace)

	// Register automatic log dumper on failure. Since this is registered after vcluster setup,
	// it will run BEFORE the vcluster teardown cleanups (LIFO order), ensuring we capture the
	// container logs before the namespace is destroyed.
	t.Cleanup(func() {
		if t.Failed() {
			dumpPodLogsOnFailure(t, ctx, vcKubeconfigPath, fixture.namespace)
		}
	})

	ensureDeploymentHealthy(t, ctx, vclusterClient, fixture.namespace, releaseName)

	fixture.vucClient = vclusterClient
	fixture.vclusterKubeconfig = vcKubeconfigPath

	return fixture
}

// setupVCluster spins up a vcluster in the host namespace, gets its kubeconfig, and installs the extension-provider.
func setupVCluster(t *testing.T, ctx context.Context, hostClient client.WithWatch, hostNamespace, vclusterName string, scheme *runtime.Scheme, hostKubeconfig string) (client.WithWatch, string) {
	const portForwardReconnectDelay = 1 * time.Second

	values := map[string]interface{}{
		"controlPlane": map[string]interface{}{
			"distro": map[string]interface{}{
				"k8s": map[string]interface{}{
					"image": map[string]interface{}{
						"tag": *vclusterK8sTag,
					},
				},
			},
			"advanced": map[string]interface{}{
				"defaultImageRegistry": *privateRegistry,
				"serviceAccount": map[string]interface{}{
					"imagePullSecrets": []interface{}{
						map[string]interface{}{
							"name": "harbor-registry-" + ptr.Deref(commitHash, ""),
						},
					},
				},
			},
			// Disable persistence for vcluster to avoid volume attachment issues
			"statefulSet": map[string]interface{}{
				"persistence": map[string]interface{}{
					"volumeClaim": map[string]interface{}{
						"enabled": false,
					},
				},
			},
		},
		// Enforce DNS fallback to the physical host cluster's CoreDNS
		// This bypasses host-node UDP traffic drops in the CI environment, allowing
		// workloads inside the vCluster to reliably resolve external URLs.
		"networking": map[string]interface{}{
			"advanced": map[string]interface{}{
				"fallbackHostCluster": true,
			},
		},
	}

	t.Logf("Installing Vcluster %q in namespace %q", vclusterName, hostNamespace)
	if _, err := helm.InstallOrUpgrade(helm.InstallOptions{
		ChartPath:      *vclusterChart,
		KubeconfigPath: hostKubeconfig,
		ReleaseName:    vclusterName,
		Namespace:      hostNamespace,
		Values:         values,
	}); err != nil {
		t.Fatalf("cannot install vcluster helm chart %v", err)
	}

	t.Cleanup(func() {
		t.Logf("Cleaning up vcluster %q in namespace %q", vclusterName, hostNamespace)
		if _, err := helm.Uninstall(helm.UninstallOptions{
			KubeconfigPath: hostKubeconfig,
			ReleaseName:    vclusterName,
			Namespace:      hostNamespace,
			Wait:           true,
		}); err != nil {
			t.Logf("unable to clean up vcluster %v", err)
		}
	})

	// Get kubeconfig secret
	secretName := "vc-" + vclusterName
	vcSecret := &corev1.Secret{}

	// Wait for secret to be created
	if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := hostClient.Get(ctx, client.ObjectKey{Namespace: hostNamespace, Name: secretName}, vcSecret)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil // retry
			}
			return false, err // stop on other errors
		}
		return true, nil // found
	}); err != nil {
		t.Fatalf("timeout waiting for vcluster secret %s: %v", secretName, err)
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(vcSecret.Data["config"])
	if err != nil {
		t.Fatalf("cannot build rest config from vcluster kubeconfig: %v", err)
	}
	restConfig.QPS = 100.0
	restConfig.Burst = 150

	// Wait for the vcluster pods to be ready before port-forwarding
	if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		vcPods := &corev1.PodList{}
		if err := hostClient.List(ctx, vcPods, client.InNamespace(hostNamespace), client.MatchingLabels{"app": "vcluster", "release": vclusterName}); err != nil {
			return false, nil
		}
		if len(vcPods.Items) == 0 {
			return false, nil
		}
		for _, p := range vcPods.Items {
			ready := false
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					ready = true
					break
				}
			}
			if !ready {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("vcluster pods not ready: %v", err)
	}

	// Find an available port on localhost to avoid port collisions when tests run in parallel.
	// If `kubectl port-forward` uses a hardcoded port (e.g. 8443) and it's already in use
	// by another test, `kubectl port-forward` crashes and the Go client will fail to connect
	// with a "connection refused" error (e.g. dial tcp [::1]:8443: connect: connection refused).
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to find an available port for vcluster: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Start kubectl port-forward in a self-healing loop.
	// `kubectl port-forward` processes are sometimes unstable and can drop connections,
	// especially in resource-constrained CI environments. If it crashes mid-test without a
	// restart loop, subsequent API requests will fail with "use of closed network connection"
	// or "EOF" errors. This loop repeatedly attempts to re-establish the tunnel if it exits.
	pfCtx, pfCancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case <-pfCtx.Done():
				return
			default:
				portForwardCmd := exec.CommandContext(pfCtx, "kubectl", "--kubeconfig", hostKubeconfig, "port-forward", "svc/"+vclusterName, fmt.Sprintf("%d:443", port), "-n", hostNamespace)
				output, err := portForwardCmd.CombinedOutput()
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						t.Logf("port-forward exited with error: %v. Output: %s", err, string(output))
					}
				}
				time.Sleep(portForwardReconnectDelay)
			}
		}
	}()

	t.Cleanup(func() {
		t.Logf("Stopping kubectl port-forward")
		pfCancel()
	})

	// Override the host in the REST config to use the new port-forwarded localhost
	restConfig.Host = fmt.Sprintf("https://localhost:%d", port)

	vclusterClient, err := client.NewWithWatch(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("cannot create client for vcluster %v", err)
	}

	// Wait for the vcluster apiserver to be reachable securely
	if err := wait.PollUntilContextTimeout(ctx, 3*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := vclusterClient.RESTMapper().RESTMapping(schema.GroupKind{Group: "", Kind: "Pod"}, "v1")
		if err != nil {
			return false, nil // wait for apiserver
		}
		return true, nil
	}); err != nil {
		t.Fatalf("vcluster apiserver not reachable via port-forward: %v", err)
	}

	// Write vcluster kubeconfig to file for helm charts deployed inside vcluster.
	// The raw kubeconfig from the vcluster secret hardcodes the server address to https://localhost:8443.
	// Because we use a dynamically allocated port for port-forwarding to avoid collisions,
	// we must parse the config, update the cluster.Server URL to use our dynamic port,
	// and re-serialize it before saving so external tools like Helm connect to the correct tunnel.
	apiConfig, err := clientcmd.Load(vcSecret.Data["config"])
	if err != nil {
		t.Fatalf("failed to parse vcluster kubeconfig: %v", err)
	}
	for _, cluster := range apiConfig.Clusters {
		cluster.Server = fmt.Sprintf("https://localhost:%d", port)
	}
	modifiedKubeconfig, err := clientcmd.Write(*apiConfig)
	if err != nil {
		t.Fatalf("failed to serialize modified vcluster kubeconfig: %v", err)
	}

	tmpKubeConfigFile, err := os.CreateTemp("", "vcluster-kubeconfig-")
	if err != nil {
		t.Fatalf("cannot create temp kubeconfig file: %v", err)
	}
	if _, err := tmpKubeConfigFile.Write(modifiedKubeconfig); err != nil {
		t.Fatalf("cannot write to temp kubeconfig file: %v", err)
	}
	tmpKubeConfigFile.Close()
	t.Cleanup(func() { os.Remove(tmpKubeConfigFile.Name()) })

	return vclusterClient, tmpKubeConfigFile.Name()
}

// createTestFixture initializes the commonTestFixture with clients and namespace.
func createTestFixture(t *testing.T, ctx context.Context, gdcClient *gdcloud.TestingClient) *commonTestFixture {
	// Generate go client for the user cluster
	scheme := runtime.NewScheme()
	if err := extensionsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register extensionsv1 scheme %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register corev1 scheme %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register appsv1 scheme %v", err)
	}
	if err := machinev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register machinev1alpha1 scheme %v", err)
	}
	// Register Gardener Extension API
	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register Gardener Extensions scheme %v", err)
	}
	if err := druidv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register ETCD druid API scheme %v", err)
	}
	if err := vmv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register vmv1 scheme %v", err)
	}
	if err := ipamv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register ipamv1 scheme %v", err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register networkingv1 scheme %v", err)
	}
	// Register Gardener Extension API
	if err := extensionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register Gardener Extensions scheme %v", err)
	}
	// Register Gardener Resources API
	if err := resourcesv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register Gardener Resources scheme %v", err)
	}
	// Register VPA API
	if err := vpaautoscalingv1.AddToScheme(scheme); err != nil {
		t.Fatalf("unable to register VPA API scheme %v", err)
	}
	// Register GDCH provider API
	gdcinstall.Install(scheme)
	vucClient, err := gdc.GetUserClusterClient(gdcClient, *zone, *project, *vuc, scheme)
	if err != nil {
		t.Fatalf("cannot create client for User Cluster %v", err)
	}

	// Generate go client for the global api server
	globalSchema := runtime.NewScheme()
	if err := globalnetworkingv1.AddToScheme(globalSchema); err != nil {
		t.Fatalf("unable to register globalnetworkingv1 scheme %v", err)
	}
	if err := ipamglobalv1.AddToScheme(globalSchema); err != nil {
		t.Fatalf("unable to register ipamglobalv1 scheme %v", err)
	}
	if err := globalv1.AddToScheme(globalSchema); err != nil {
		t.Fatalf("unable to register globalv1 scheme %v", err)
	}
	globalClient, err := gdc.GetGlobalClient(gdcClient, globalSchema)
	if err != nil {
		t.Fatalf("cannot create client for Global API %v", err)
	}

	// Generate go client for the management cluster
	mgmtClient, err := GetManagementClient(scheme, *zone)
	if err != nil {
		t.Fatalf("failed to create management client: %v", err)
	}

	// Setup Namespace
	namespace := setupNamespace(t, ctx, vucClient)

	rawCA, err := os.ReadFile(*cafile)
	if err != nil {
		t.Fatalf("cannot read cafile at %s: %v", *cafile, err)
	}

	// 2. Base64 encode the content
	encodedCA := base64.StdEncoding.EncodeToString(rawCA)

	// Create gdchConfig
	gdchConfig := &gdcclient.OrgClusterConfig{
		CAData:        encodedCA,
		OrgClusterURL: fmt.Sprintf("https://global-api.%s.%s.%s", *org, *zone, *labURL),
	}

	return &commonTestFixture{
		scheme:       scheme,
		gdcClient:    gdcClient,
		vucClient:    vucClient,
		globalClient: globalClient,
		mgmtClient:   mgmtClient,
		gdchConfig:   gdchConfig,
		namespace:    namespace,
		project:      *project,
	}
}

// setupNamespace creates a new namespace for the test and registers a cleanup function.
func setupNamespace(t *testing.T, ctx context.Context, c client.WithWatch) string {
	namespace := "extension-provider-" + ptr.Deref(commitHash, "")

	if err := kubernetes.CreateNamespace(ctx, c, namespace); err != nil {
		t.Fatalf("cannot create namespace %q, %v", namespace, err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up namespace %q", namespace)
		kubernetes.CleanupResources(t, c, namespace)
	})
	return namespace
}

// Create Secret for ImagePullSecret and register clean up
// setupImagePullSecret creates a secret for pulling images from the registry.
func setupImagePullSecret(t *testing.T, ctx context.Context, c client.Client, namespace string) {
	cred, err := os.ReadFile(*imagePullCredential)
	if err != nil {
		t.Fatalf("cannot read imagePullCredential file %v", err)
	}
	imagePullSecretName := "harbor-registry-" + ptr.Deref(commitHash, "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      imagePullSecretName,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			".dockerconfigjson": cred,
		},
		Type: "kubernetes.io/dockerconfigjson",
	}
	if err := c.Create(ctx, secret); err != nil {
		t.Fatalf("cannot create Secret for imagePullCredential %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up secret %q in namespace %q", secret.Name, secret.Namespace)
		if err := c.Delete(ctx, secret); err != nil {
			t.Logf("unable to clean up Secret for imagePullCredential, %v", err)
		}
	})
}

// setupHelmChart installs the extension provider helm chart.
func setupHelmChart(t *testing.T, kubeconfig, namespace string) string {
	releaseName := "extension-provider-chart-" + ptr.Deref(commitHash, "")
	imageURL, imageVer, err := parseImageTag(*imageTag)
	if err != nil {
		t.Fatalf("cannot parse image tag %v", err)
	}
	imagePullSecretName := "harbor-registry-" + ptr.Deref(commitHash, "")
	imageVectorOverwrite := `images:
- name: csi-provisioner
  repository: quay.io/openshift/origin-csi-external-provisioner
  tag: 4.18.0
- name: csi-attacher
  repository: quay.io/openshift/origin-csi-external-attacher
  tag: 4.18.0
- name: csi-liveness-probe
  repository: quay.io/openshift/origin-csi-livenessprobe
  tag: 4.18.0
- name: csi-node-driver-registrar
  repository: quay.io/openshift/origin-csi-node-driver-registrar
  tag: 4.18.0
- name: csi-snapshotter
  repository: gcr.io/private-cloud-ci/csi-snapshotter
  tag: v6.3.2-gke.4
- name: csi-snapshot-controller
  repository: gcr.io/anthos-baremetal-release/snapshot-controller
  tag: v8.0.1-gke.8
- name: csi-resizer
  repository: gcr.io/private-cloud-staging/csi-resizer
  tag: v1.10.0-gke.1
`
	values := map[string]interface{}{
		"image": map[string]interface{}{
			"repository": imageURL,
			"tag":        imageVer,
			"pullPolicy": "Always",
		},
		"imagePullSecrets": []map[string]interface{}{
			{
				"name": imagePullSecretName,
			},
		},
		"skipPriorityClassName": true,
		"imageVectorOverwrite":  imageVectorOverwrite,
		// enable all webhooks
		"disableWebhooks": []interface{}{},
		"controllers": map[string]interface{}{
			"ignoreOperationAnnotation": true,
		},
		"config": map[string]interface{}{
			"etcd": map[string]interface{}{
				"storage": map[string]interface{}{
					"className": "performance-rwo",
					"capacity":  "10Gi",
				},
			},
		},
	}

	if _, err = helm.InstallOrUpgrade(helm.InstallOptions{
		ChartPath:      *chartPackage,
		KubeconfigPath: kubeconfig,
		ReleaseName:    releaseName,
		Namespace:      namespace,
		Values:         values,
	}); err != nil {
		t.Fatalf("cannot install helm chart %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up helm chart %q in namespace %q", releaseName, namespace)
		if _, err := helm.Uninstall(helm.UninstallOptions{
			KubeconfigPath: kubeconfig,
			ReleaseName:    releaseName,
			Namespace:      namespace,
			Wait:           true,
		}); err != nil {
			t.Logf("unable to clean up helm chart for extension-provider, %v", err)
		}
	})
	return releaseName
}

// ensureDeploymentHealthy waits for the deployment and its pods to become ready.
func ensureDeploymentHealthy(t *testing.T, ctx context.Context, c client.WithWatch, namespace, releaseName string) {
	if err := kubernetes.WaitForDeploymentReady(ctx, c, namespace, "gardener-extension-provider-gdch", time.Minute*8); err != nil {
		t.Fatalf("gardener-extension-provider-gdch deployment in %q is not Ready in 8 minutes %v", namespace, err)
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": releaseName}); err != nil {
		t.Fatalf("cannot list pods for extension-provider deployment in namespace %q, %v", namespace, err)
	}
	for _, p := range pods.Items {
		if err := kubernetes.WaitForPodReady(ctx, c, namespace, p.Name, time.Minute); err != nil {
			t.Fatalf("pod %q in %q namespace is not Ready in one minute, %v", p.Name, namespace, err)
		}
	}
}

func parseImageTag(imageTag string) (string, string, error) {
	named, err := reference.ParseNormalizedNamed(imageTag)
	if err != nil {
		return "", "", err
	}

	if tagged, ok := named.(reference.Tagged); ok {
		return reference.TrimNamed(named).String(), tagged.Tag(), nil
	}
	return named.String(), "latest", nil
}

// GetManagementClient creates a client for the management cluster.
func GetManagementClient(scheme *runtime.Scheme, zone string) (client.WithWatch, error) {
	// Use Service Account and OrgClusterConfig
	caData, err := os.ReadFile(*cafile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}

	saBytes, err := os.ReadFile(*safile)
	if err != nil {
		return nil, fmt.Errorf("failed to read Service Account file: %w", err)
	}

	var sa auth.ServiceAccount
	if err := json.Unmarshal(saBytes, &sa); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Service Account: %w", err)
	}

	orgClusterCfg := &gdcclient.OrgClusterConfig{
		OrgClusterURL: fmt.Sprintf("https://management-kube.apiserver.%s.%s.%s", *org, zone, *labURL),
		CAData:        base64.StdEncoding.EncodeToString(caData),
	}

	c, err := gdcclient.Get(orgClusterCfg, &sa, scheme)
	if err != nil {
		return nil, err
	}
	cw, ok := c.(client.WithWatch)
	if !ok {
		return nil, fmt.Errorf("management client does not support Watch")
	}
	return cw, nil
}

func dumpPodLogsOnFailure(t *testing.T, ctx context.Context, kubeconfigPath, namespace string) {
	t.Helper()
	t.Logf("--- DUMPING POD LOGS FOR NAMESPACE %q ---", namespace)
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		t.Logf("Failed to build rest config for log dumping: %v", err)
		return
	}
	restConfig.Timeout = 10 * time.Second

	clientset, err := kubernetesclientset.NewForConfig(restConfig)
	if err != nil {
		t.Logf("Failed to create clientset for log dumping: %v", err)
		return
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("Failed to list pods in namespace %q: %v", namespace, err)
		return
	}

	for _, pod := range pods.Items {
		t.Logf("Pod: %s, Status: %s, Message: %s, Reason: %s", pod.Name, pod.Status.Phase, pod.Status.Message, pod.Status.Reason)
		for _, container := range pod.Spec.Containers {
			t.Logf("  Container: %s", container.Name)
			data, err := clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container.Name}).DoRaw(ctx)
			if err != nil {
				t.Logf("    Failed to get logs for container %s: %v", container.Name, err)
				continue
			}
			t.Logf("    Logs for %s/%s:\n%s", pod.Name, container.Name, string(data))
		}
	}
	t.Log("--- END OF POD LOGS DUMP ---")
}
