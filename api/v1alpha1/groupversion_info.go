// Package v1alpha1 contains the placement API for agent sandboxes.
//
// The group is `placement.agents.x-k8s.io`, matching the convention upstream
// already uses — `agents.x-k8s.io` for the core Sandbox API and
// `extensions.agents.x-k8s.io` for SandboxClaim/Template/WarmPool. Placement is
// the same shape of thing as those extensions: an optional layer over the core
// API rather than a change to it. Using the group signals where this is
// intended to live if the SIG adopts it; nothing here depends on that outcome.
//
// +kubebuilder:object:generate=true
// +groupName=placement.agents.x-k8s.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group and version for this API.
	GroupVersion = schema.GroupVersion{Group: "placement.agents.x-k8s.io", Version: "v1alpha1"}

	// SchemeBuilder registers these types with a runtime scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds these types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
