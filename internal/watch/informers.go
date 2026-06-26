/*
SPDX-License-Identifier: Apache-2.0

Copyright 2025 ConfigButler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watch

// This file implements the watch-mode live-event path: long-lived Kubernetes informers
// per active GVR replace the audit tail. When --capture-mode=watch the operator needs no
// kube-apiserver audit webhook and no Valkey/Redis dependency; informer events drive per-event
// git commits just as the audit tail does in audit mode.
//
// Integration with the type lifecycle:
//   - TypeActivated → startWatchModeInformerForType: informer started for that GVR.
//   - TypeRemoved   → stopWatchModeInformerForType: informer cancelled and evicted.
//
// Watch mode is enabled when Manager.WatchMode is true (set from --capture-mode=watch in cmd).
// In audit mode this file is a no-op: every method returns immediately when WatchMode is false.

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/ConfigButler/gitops-reverser/internal/git"
	"github.com/ConfigButler/gitops-reverser/internal/sanitize"
	"github.com/ConfigButler/gitops-reverser/internal/types"
)

// WatchModeEnabled reports whether watch mode is active.
func (m *Manager) WatchModeEnabled() bool {
	return m.WatchMode
}

// startWatchModeInformers starts informers for all currently active GVRs. Called once at
// manager start in watch mode after the initial catalog + type-table refresh.
func (m *Manager) startWatchModeInformers(ctx context.Context, log logr.Logger) {
	if !m.WatchModeEnabled() {
		return
	}
	for _, table := range m.allWatchedTypeTables() {
		for _, wt := range table.Types {
			m.startWatchModeInformerForType(ctx, log, wt.GVR)
		}
	}
}

// startWatchModeInformerForType starts a dynamic informer for gvr if one is not already
// running. Idempotent: a repeat call for a running informer is a no-op.
func (m *Manager) startWatchModeInformerForType(ctx context.Context, log logr.Logger, gvr schema.GroupVersionResource) {
	if !m.WatchModeEnabled() {
		return
	}
	m.watchInformersMu.Lock()
	defer m.watchInformersMu.Unlock()

	if m.watchInformers == nil {
		m.watchInformers = make(map[schema.GroupVersionResource]context.CancelFunc)
	}
	if _, running := m.watchInformers[gvr]; running {
		return
	}

	dc := m.dynamicClientFromConfig(log)
	if dc == nil {
		log.Error(nil, "no dynamic client available; cannot start watch-mode informer", "gvr", gvr.String())
		return
	}

	informerCtx, cancel := context.WithCancel(ctx) // cancel stored in watchInformers, called by stopWatchModeInformerForType
	m.watchInformers[gvr] = cancel

	go m.runWatchModeInformer(informerCtx, log, dc, gvr)
	log.Info("Watch-mode informer started", "gvr", gvr.String())
}

// stopWatchModeInformerForType cancels and evicts the informer for gvr. No-op when not running.
func (m *Manager) stopWatchModeInformerForType(gvr schema.GroupVersionResource) {
	m.watchInformersMu.Lock()
	defer m.watchInformersMu.Unlock()
	if cancel, ok := m.watchInformers[gvr]; ok {
		cancel()
		delete(m.watchInformers, gvr)
	}
}

// stopAllWatchModeInformers cancels every running informer. Called on shutdown.
func (m *Manager) stopAllWatchModeInformers() {
	m.watchInformersMu.Lock()
	defer m.watchInformersMu.Unlock()
	for gvr, cancel := range m.watchInformers {
		cancel()
		delete(m.watchInformers, gvr)
	}
}

// runWatchModeInformer creates a dynamic informer for gvr and blocks until ctx is cancelled.
func (m *Manager) runWatchModeInformer(
	ctx context.Context,
	log logr.Logger,
	dc dynamic.Interface,
	gvr schema.GroupVersionResource,
) {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dc, 0)
	informer := factory.ForResource(gvr).Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			m.handleWatchModeEvent(ctx, log, obj, gvr, "CREATE")
		},
		UpdateFunc: func(_, newObj interface{}) {
			m.handleWatchModeEvent(ctx, log, newObj, gvr, "UPDATE")
		},
		DeleteFunc: func(obj interface{}) {
			// Handle tombstones from the informer cache.
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			m.handleWatchModeDeleteEvent(ctx, log, obj, gvr)
		},
	}); err != nil {
		log.Error(err, "failed to add event handler to watch-mode informer", "gvr", gvr.String())
		return
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	log.V(1).Info("watch-mode informer synced", "gvr", gvr.String())
	<-ctx.Done()
}

// handleWatchModeEvent converts an informer add/update into a git.Event and routes it.
func (m *Manager) handleWatchModeEvent(
	ctx context.Context,
	log logr.Logger,
	obj interface{},
	gvr schema.GroupVersionResource,
	operation string,
) {
	u, ok := toUnstructured(obj)
	if !ok {
		return
	}
	sanitized := sanitize.Sanitize(u)
	id := types.NewResourceIdentifier(gvr.Group, gvr.Version, gvr.Resource, u.GetNamespace(), u.GetName())
	ev := git.Event{
		Object:     sanitized,
		Identifier: id,
		Operation:  operation,
	}
	m.routeWatchModeEvent(ctx, log, gvr, ev)
}

// handleWatchModeDeleteEvent converts an informer delete into a git.Event and routes it.
func (m *Manager) handleWatchModeDeleteEvent(
	ctx context.Context,
	log logr.Logger,
	obj interface{},
	gvr schema.GroupVersionResource,
) {
	u, ok := toUnstructured(obj)
	if !ok {
		return
	}
	id := types.NewResourceIdentifier(gvr.Group, gvr.Version, gvr.Resource, u.GetNamespace(), u.GetName())
	ev := git.Event{
		Identifier: id,
		Operation:  "DELETE",
	}
	m.routeWatchModeEvent(ctx, log, gvr, ev)
}

// routeWatchModeEvent fans one informer event to every GitTarget that watches the GVR,
// applying namespace-scope filtering but no watermark gate (all informer events are live
// by construction — they arrive only after the initial snapshot reconcile has seeded git).
func (m *Manager) routeWatchModeEvent(
	ctx context.Context,
	log logr.Logger,
	gvr schema.GroupVersionResource,
	ev git.Event,
) {
	if m.EventRouter == nil {
		return
	}
	for _, table := range m.allWatchedTypeTables() {
		watched := watchedTypeForGVR(table, gvr)
		if watched == nil {
			continue
		}
		if m.EventRouter.GetGitTargetEventStream(table.GitDest) == nil {
			continue
		}
		path, ok := m.gitTargetPath(ctx, table.GitDest)
		if !ok {
			continue
		}
		inScope := namespaceScopePredicate(watched.SnapshotNamespaces())
		if !inScope(ev.Identifier.Namespace) {
			continue
		}
		routed := ev
		routed.Path = path
		if err := m.EventRouter.RouteToGitTargetEventStream(routed, table.GitDest); err != nil {
			log.V(1).Info("watch-mode event route failed",
				"gitDest", table.GitDest.String(), "gvr", gvr.String(), "err", err.Error())
		}
	}
}

// startWatchModePeriodicReconcile starts a goroutine that fires a forced full re-snapshot on
// WatchModeReconcileInterval to self-heal any events missed during informer restarts or cache
// re-lists. A zero or negative interval disables the feature.
func (m *Manager) startWatchModePeriodicReconcile(ctx context.Context, log logr.Logger) {
	if !m.WatchModeEnabled() || m.WatchModeReconcileInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(m.WatchModeReconcileInterval)
		defer ticker.Stop()
		log.Info("Watch-mode periodic reconciliation enabled", "interval", m.WatchModeReconcileInterval.String())
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Info("Watch-mode periodic reconciliation triggered")
				if err := m.ReconcileForRuleChange(ctx); err != nil {
					log.Error(err, "Watch-mode periodic reconciliation failed")
				}
			}
		}
	}()
}

// toUnstructured extracts an *unstructured.Unstructured from an informer callback object.
func toUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok && u != nil
}
