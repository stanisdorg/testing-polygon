#!/usr/bin/env bash
# ============================================================
# FulfilBox — K8s Local Setup (kind + Helm)
# ============================================================
# Usage: ./k8s-setup.sh [create|destroy]
# ============================================================

set -euo pipefail

CLUSTER_NAME="fulfilbox"
HELM_RELEASE="fulfilbox"
NAMESPACE="fulfilbox"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

check_prereqs() {
    local ok=true
    for cmd in docker kind helm kubectl; do
        if ! command -v "$cmd" &>/dev/null; then
            log_error "$cmd not found. Install it first."
            ok=false
        fi
    done
    if [ "$ok" = false ]; then
        log_error "Missing prerequisites. Install: brew install kind helm kubectl"
        exit 1
    fi
}

create_cluster() {
    log_info "Creating kind cluster: $CLUSTER_NAME"

    # Delete existing cluster if any
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warn "Cluster $CLUSTER_NAME already exists. Deleting..."
        kind delete cluster --name "$CLUSTER_NAME"
    fi

    # Create kind cluster with extra ports
    cat > /tmp/kind-config.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${CLUSTER_NAME}
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 30080
        hostPort: 30080
        protocol: TCP
      - containerPort: 31080
        hostPort: 31080
        protocol: TCP
  - role: worker
  - role: worker
EOF

    kind create cluster --config /tmp/kind-config.yaml
    log_info "Cluster created!"

    # Connect docker-compose student-portal to kind network
    log_info "Connecting student-portal to kind network..."
    PORTAL_CONTAINER=$(docker ps --format '{{.Names}}' | grep student-portal | head -1)
    if [ -n "$PORTAL_CONTAINER" ]; then
        docker network connect kind "$PORTAL_CONTAINER" 2>/dev/null || true
        log_info "Connected!"
    fi

    # Load images
    log_info "Building and loading Docker images..."
    cd "$(dirname "$0")"

    for svc in order-service event-consumer-service simulation-service ws-gateway student-portal; do
        log_info "Building $svc..."
        docker build -t "fulfilbox/$svc:latest" "services/$svc" -q
        kind load docker-image "fulfilbox/$svc:latest" --name "$CLUSTER_NAME"
    done

    log_info "All images loaded!"

    # Create namespace
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    # Deploy with Helm
    log_info "Deploying FulfilBox with Helm..."
    helm upgrade --install "$HELM_RELEASE" ./charts/fulfilbox \
        --namespace "$NAMESPACE" \
        --create-namespace \
        --set imagePullPolicy=Never \
        --wait --timeout 300s

    log_info "FulfilBox deployed!"

    # Setup K8s Dashboard
    log_info "Setting up Kubernetes Dashboard..."
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml

    # Create admin token
    cat > /tmp/dashboard-admin.yaml <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: admin-user
    namespace: kubernetes-dashboard
EOF
    kubectl apply -f /tmp/dashboard-admin.yaml

    log_info "Dashboard created!"

    # Wait for pods
    log_info "Waiting for pods to be ready..."
    kubectl wait --for=condition=Ready pods --all -n "$NAMESPACE" --timeout=120s || true

    # Print status
    echo ""
    log_info "========================================="
    log_info "  FulfilBox K8s Cluster Ready!"
    log_info "========================================="
    echo ""
    echo "  Student Portal: http://localhost:30080"
    echo "  K8s Dashboard:  run: kubectl proxy"
    echo "                  then: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
    echo ""
    echo "  Get dashboard token:"
    echo "  kubectl -n kubernetes-dashboard create token admin-user"
    echo ""
    echo "  Useful commands:"
    echo "    kubectl get pods -n fulfilbox"
    echo "    kubectl get deployments -n fulfilbox"
    echo "    kubectl get hpa -n fulfilbox"
    echo "    kubectl logs -n fulfilbox <pod-name>"
    echo "    kubectl exec -it -n fulfilbox <pod-name> -- sh"
    echo ""
    echo "  Destroy: ./k8s-setup.sh destroy"
    echo "========================================="
}

destroy_cluster() {
    log_warn "Destroying kind cluster: $CLUSTER_NAME"
    kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
    log_info "Cluster destroyed!"
}

status() {
    log_info "Cluster status:"
    kind get clusters 2>/dev/null || echo "  No kind clusters found"
    echo ""
    if kubectl cluster-info &>/dev/null; then
        log_info "Pods in fulfilbox namespace:"
        kubectl get pods -n fulfilbox 2>/dev/null || echo "  Namespace not found"
        echo ""
        log_info "Deployments:"
        kubectl get deployments -n fulfilbox 2>/dev/null || true
        echo ""
        log_info "HPA:"
        kubectl get hpa -n fulfilbox 2>/dev/null || true
    fi
}

case "${1:-create}" in
    create)
        check_prereqs
        create_cluster
        ;;
    destroy)
        destroy_cluster
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {create|destroy|status}"
        exit 1
        ;;
esac
