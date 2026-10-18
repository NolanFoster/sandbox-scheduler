package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
)

func TestASecretMapsToTheProvidersThatReferenceIt(t *testing.T) {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	withRef := func(name, secret string) *v1alpha1.SandboxProvider {
		p := &v1alpha1.SandboxProvider{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if secret != "" {
			p.Spec.CredentialsRef = &corev1.LocalObjectReference{Name: secret}
		}
		return p
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		withRef("civo", "shared"), withRef("gke", "shared"),
		withRef("hosted", "other"), withRef("bare", ""),
	).Build()
	r := &SandboxProviderReconciler{Client: c, SecretNamespace: "sched"}

	got := r.providersForSecret(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "sched"},
	})
	names := map[string]bool{}
	for _, req := range got {
		names[req.Name] = true
	}
	if len(got) != 2 || !names["civo"] || !names["gke"] {
		t.Fatalf("mapped to %v, want civo and gke", got)
	}

	// Same name, wrong namespace: credentials are only read from the
	// scheduler's own, so a Secret elsewhere must not trigger anything.
	if got := r.providersForSecret(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
	}); len(got) != 0 {
		t.Fatalf("a secret outside the secret namespace mapped to %v", got)
	}
}
