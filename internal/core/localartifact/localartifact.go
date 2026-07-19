// Package localartifact defines the durable, version-aware source boundary
// used for conditional revalidation. Persistence adapters must fail closed for
// expired, denied, corrupt, or missing representations.
package localartifact

import (
	"context"
	"time"

	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

type Version struct {
	URL                  string
	EffectiveURL         string
	Body                 []byte
	ContentHash          string
	ETag                 string
	LastModified         string
	MIME                 string
	PolicyAgent          string
	FetchedAt            time.Time
	ObservedAt           time.Time
	ValidatedAt          time.Time
	ExpiresAt            *time.Time
	SafetyClassification localcorpus.SafetyClassification
	IndexingDisposition  localcorpus.IndexingDisposition
}

type Observation struct {
	URL          string
	ObservedAt   time.Time
	ValidatedAt  time.Time
	ETag         string
	LastModified string
	ExpiresAt    *time.Time
}

// VersionInfo describes one immutable representation committed for a URL in
// archive mode. Current validation metadata lives on the URL pointer and may
// advance independently through conditional revalidation.
type VersionInfo struct {
	ContentHash          string
	EffectiveURL         string
	MIME                 string
	PolicyAgent          string
	FetchedAt            time.Time
	ObservedAt           time.Time
	ValidatedAt          time.Time
	ExpiresAt            *time.Time
	SafetyClassification localcorpus.SafetyClassification
}

// History is the ordered set of distinct representations committed for a URL.
// CurrentHash selects the representation exposed by Current.
type History struct {
	URL                 string
	CurrentHash         string
	Versions            []VersionInfo
	ObservedAt          time.Time
	IndexingDisposition localcorpus.IndexingDisposition
}

// ArchiveReader exposes immutable historical representations when the backing
// store was opened in archive mode.
type ArchiveReader interface {
	History(context.Context, string) (History, bool, error)
	Version(context.Context, string, string) (Version, bool, error)
}

type Store interface {
	Current(context.Context, string) (Version, bool, error)
	Put(context.Context, Version) error
	Revalidate(context.Context, Observation) error
	Revoke(context.Context, string, localcorpus.IndexingDisposition, time.Time) error
}
