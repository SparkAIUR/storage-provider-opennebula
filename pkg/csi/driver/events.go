package driver

const (
	eventReasonDatastoreSelected            = "DatastoreSelected"
	eventReasonDatastoreFallback            = "DatastoreFallback"
	eventReasonDatastoreRejected            = "DatastoreRejected"
	eventReasonSnapshotCreated              = "SnapshotCreated"
	eventReasonSnapshotDeleted              = "SnapshotDeleted"
	eventReasonCloneCreated                 = "CloneCreated"
	eventReasonExpandDeferred               = "ExpandDeferred"
	eventReasonAttachValidated              = "AttachValidated"
	eventReasonHotplugTimeoutScaled         = "HotplugTimeoutScaled"
	eventReasonHotplugRecoveryPath          = "HotplugRecoveryPath"
	eventReasonHotplugNodeBusy              = "HotplugNodeBusy"
	eventReasonHotplugCooldown              = "HotplugCooldown"
	eventReasonDeviceDiscoverySlow          = "DeviceDiscoverySlow"
	eventReasonDeviceDiscoveryTimeout       = "DeviceDiscoveryTimeout"
	eventReasonLocalImmediateBindingWarning = "LocalImmediateBindingWarning"
	eventReasonPreflightFailed              = "PreflightFailed"
)
