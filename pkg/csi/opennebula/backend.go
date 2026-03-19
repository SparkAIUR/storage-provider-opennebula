package opennebula

import (
	"fmt"
	"strings"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
)

type DeploymentMode string

const (
	DeploymentModeUnknown DeploymentMode = "unknown"
	DeploymentModeCeph    DeploymentMode = "ceph"
	DeploymentModeSSH     DeploymentMode = "ssh"
)

const (
	datastoreTypeLocal = "local"
	datastoreTypeCeph  = "ceph"

	cephAttrPoolName   = "POOL_NAME"
	cephAttrHost       = "CEPH_HOST"
	cephAttrUser       = "CEPH_USER"
	cephAttrSecret     = "CEPH_SECRET"
	cephAttrConf       = "CEPH_CONF"
	cephAttrKey        = "CEPH_KEY"
	cephAttrBridgeList = "BRIDGE_LIST"
	cephAttrRBDFormat  = "RBD_FORMAT"
	cephAttrECPoolName = "EC_POOL_NAME"
	cephAttrDiskType   = "DISK_TYPE"

	cephDiskTypeRBD = "RBD"
)

var cephRequiredAttrs = []string{
	cephAttrPoolName,
	cephAttrHost,
	cephAttrUser,
	cephAttrSecret,
	cephAttrBridgeList,
}

type DatastoreBackendProfile struct {
	Type              string
	RequiredAttrs     []string
	AllowedImageModes []DeploymentMode
	validate          func(datastoreSchema.Datastore) error
}

func (p DatastoreBackendProfile) ValidateDatastore(ds datastoreSchema.Datastore) error {
	if p.validate == nil {
		return nil
	}

	return p.validate(ds)
}

type DatastoreEnvironmentReport struct {
	ImageDatastore  Datastore
	SystemDatastore *Datastore
	DeploymentMode  DeploymentMode
	Warnings        []string
}

type CephDatastoreAttributes struct {
	PoolName   string
	CephHost   string
	CephUser   string
	CephSecret string
	CephConf   string
	CephKey    string
	DiskType   string
	BridgeList string
	RBDFormat  string
	ECPoolName string
}

func GetDatastoreBackendProfile(typ string) (DatastoreBackendProfile, error) {
	switch normalizeAllowedDatastoreType(typ) {
	case datastoreTypeLocal:
		return DatastoreBackendProfile{
			Type:              datastoreTypeLocal,
			AllowedImageModes: []DeploymentMode{DeploymentModeUnknown, DeploymentModeSSH, DeploymentModeCeph},
		}, nil
	case datastoreTypeCeph:
		return DatastoreBackendProfile{
			Type:              datastoreTypeCeph,
			RequiredAttrs:     append([]string(nil), cephRequiredAttrs...),
			AllowedImageModes: []DeploymentMode{DeploymentModeCeph, DeploymentModeSSH},
			validate:          validateCephImageDatastore,
		}, nil
	default:
		return DatastoreBackendProfile{}, &datastoreConfigError{message: fmt.Sprintf("unsupported datastore backend %q", typ)}
	}
}

func extractCephDatastoreAttributes(ds datastoreSchema.Datastore) CephDatastoreAttributes {
	return CephDatastoreAttributes{
		PoolName:   getDatastoreAttribute(ds, cephAttrPoolName),
		CephHost:   getDatastoreAttribute(ds, cephAttrHost),
		CephUser:   getDatastoreAttribute(ds, cephAttrUser),
		CephSecret: getDatastoreAttribute(ds, cephAttrSecret),
		CephConf:   getDatastoreAttribute(ds, cephAttrConf),
		CephKey:    getDatastoreAttribute(ds, cephAttrKey),
		DiskType:   getDatastoreAttribute(ds, cephAttrDiskType),
		BridgeList: getDatastoreAttribute(ds, cephAttrBridgeList),
		RBDFormat:  getDatastoreAttribute(ds, cephAttrRBDFormat),
		ECPoolName: getDatastoreAttribute(ds, cephAttrECPoolName),
	}
}

func validateCephImageDatastore(ds datastoreSchema.Datastore) error {
	if !strings.EqualFold(strings.TrimSpace(ds.DSMad), datastoreTypeCeph) {
		return &datastoreConfigError{message: fmt.Sprintf("datastore %d must use DS_MAD=ceph", ds.ID)}
	}

	if !strings.EqualFold(strings.TrimSpace(ds.TMMad), datastoreTypeCeph) {
		return &datastoreConfigError{message: fmt.Sprintf("datastore %d must use TM_MAD=ceph", ds.ID)}
	}

	if normalizeDiskType(ds) != cephDiskTypeRBD {
		return &datastoreConfigError{message: fmt.Sprintf("datastore %d must use DISK_TYPE=RBD", ds.ID)}
	}

	return requireDatastoreAttributes(ds, cephRequiredAttrs...)
}

func validateCephSystemDatastore(ds datastoreSchema.Datastore) error {
	if !strings.EqualFold(strings.TrimSpace(ds.TMMad), datastoreTypeCeph) {
		return &datastoreConfigError{message: fmt.Sprintf("system datastore %d must use TM_MAD=ceph", ds.ID)}
	}

	if normalizeDiskType(ds) != cephDiskTypeRBD {
		return &datastoreConfigError{message: fmt.Sprintf("system datastore %d must use DISK_TYPE=RBD", ds.ID)}
	}

	return requireDatastoreAttributes(ds, cephRequiredAttrs...)
}

func compareCephConnectionIdentity(imageDS, systemDS datastoreSchema.Datastore) error {
	imageAttrs := extractCephDatastoreAttributes(imageDS)
	systemAttrs := extractCephDatastoreAttributes(systemDS)

	checks := []struct {
		name  string
		image string
		sys   string
	}{
		{name: cephAttrPoolName, image: imageAttrs.PoolName, sys: systemAttrs.PoolName},
		{name: cephAttrHost, image: imageAttrs.CephHost, sys: systemAttrs.CephHost},
		{name: cephAttrUser, image: imageAttrs.CephUser, sys: systemAttrs.CephUser},
		{name: cephAttrSecret, image: imageAttrs.CephSecret, sys: systemAttrs.CephSecret},
	}

	for _, check := range checks {
		if check.image != check.sys {
			return &datastoreConfigError{
				message: fmt.Sprintf("ceph datastore mismatch for %s between image datastore %d and system datastore %d", check.name, imageDS.ID, systemDS.ID),
			}
		}
	}

	optionalChecks := []struct {
		name  string
		image string
		sys   string
	}{
		{name: cephAttrConf, image: imageAttrs.CephConf, sys: systemAttrs.CephConf},
		{name: cephAttrKey, image: imageAttrs.CephKey, sys: systemAttrs.CephKey},
	}

	for _, check := range optionalChecks {
		if check.image == "" || check.sys == "" {
			continue
		}
		if check.image != check.sys {
			return &datastoreConfigError{
				message: fmt.Sprintf("ceph datastore mismatch for %s between image datastore %d and system datastore %d", check.name, imageDS.ID, systemDS.ID),
			}
		}
	}

	return nil
}

func resolveDeploymentMode(ds datastoreSchema.Datastore) DeploymentMode {
	switch strings.ToLower(strings.TrimSpace(ds.TMMad)) {
	case datastoreTypeCeph:
		return DeploymentModeCeph
	case "ssh":
		return DeploymentModeSSH
	default:
		return DeploymentModeUnknown
	}
}

func requireDatastoreAttributes(ds datastoreSchema.Datastore, keys ...string) error {
	for _, key := range keys {
		if getDatastoreAttribute(ds, key) == "" {
			return &datastoreConfigError{message: fmt.Sprintf("datastore %d is missing required attribute %s", ds.ID, key)}
		}
	}

	return nil
}

func getDatastoreAttribute(ds datastoreSchema.Datastore, key string) string {
	switch strings.ToUpper(strings.TrimSpace(key)) {
	case "DS_MAD":
		return strings.TrimSpace(ds.DSMad)
	case "TM_MAD":
		return strings.TrimSpace(ds.TMMad)
	case cephAttrDiskType:
		if strings.TrimSpace(ds.DiskType) != "" {
			return strings.TrimSpace(ds.DiskType)
		}
	}

	value, err := ds.Template.GetStr(strings.ToUpper(strings.TrimSpace(key)))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(value)
}

func normalizeDiskType(ds datastoreSchema.Datastore) string {
	return strings.ToUpper(getDatastoreAttribute(ds, cephAttrDiskType))
}
