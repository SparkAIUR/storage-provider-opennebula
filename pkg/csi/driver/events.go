package driver

const (
	eventReasonDatastoreSelected    = "DatastoreSelected"
	eventReasonDatastoreFallback    = "DatastoreFallback"
	eventReasonDatastoreRejected    = "DatastoreRejected"
	eventReasonSnapshotCreated      = "SnapshotCreated"
	eventReasonSnapshotDeleted      = "SnapshotDeleted"
	eventReasonCloneCreated         = "CloneCreated"
	eventReasonExpandDeferred       = "ExpandDeferred"
	eventReasonAttachValidated      = "AttachValidated"
	eventReasonHotplugTimeoutScaled = "HotplugTimeoutScaled"
	eventReasonHotplugNodeBusy      = "HotplugNodeBusy"
	eventReasonHotplugCooldown      = "HotplugCooldown"
	eventReasonPreflightFailed      = "PreflightFailed"
)
