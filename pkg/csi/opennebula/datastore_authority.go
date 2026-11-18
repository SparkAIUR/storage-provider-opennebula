package opennebula

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"

	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
)

type datastoreAuthorityXML struct {
	IDs  []string `xml:"ID"`
	Body string   `xml:",innerxml"`
}

func (p *PersistentDiskVolumeProvider) readDatastoreAuthorityPool(ctx context.Context) ([]datastoreAuthorityXML, error) {
	response, err := p.ctrl.Client.DatastorePoolInfo(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	var pool struct {
		XMLName xml.Name                `xml:"DATASTORE_POOL"`
		Rows    []datastoreAuthorityXML `xml:"DATASTORE"`
	}
	if err := xml.Unmarshal([]byte(response.Body()), &pool); err != nil {
		return nil, err
	}
	return pool.Rows, nil
}

func selectDatastoreAuthority(rows []datastoreAuthorityXML, datastoreID int) (datastoreSchema.Datastore, error) {
	var datastore datastoreSchema.Datastore
	var selected *datastoreAuthorityXML
	for _, row := range rows {
		matching, unknown := false, len(row.IDs) == 0
		for _, raw := range row.IDs {
			id, err := strconv.Atoi(strings.TrimSpace(raw))
			matching = matching || (err == nil && id == datastoreID)
			unknown = unknown || err != nil || id < 0
		}
		if unknown || (matching && (selected != nil || len(row.IDs) != 1)) {
			return datastore, fmt.Errorf("%w: datastore %d identity is unverified", ErrConflictingAttachmentEvidence, datastoreID)
		}
		if matching {
			selected = &row
		}
	}
	if selected == nil {
		return datastore, &datastoreConfigError{message: fmt.Sprintf("datastore %d was not found", datastoreID)}
	}
	data := []byte("<DATASTORE>" + selected.Body + "</DATASTORE>")
	var fields struct {
		Names     []string `xml:"NAME"`
		Types     []string `xml:"TYPE"`
		DSDrivers []string `xml:"DS_MAD"`
		TMDrivers []string `xml:"TM_MAD"`
		DiskTypes []string `xml:"DISK_TYPE"`
		Templates []struct {
			Types     []string `xml:"TYPE"`
			DiskTypes []string `xml:"DISK_TYPE"`
		} `xml:"TEMPLATE"`
	}
	if err := xml.Unmarshal(data, &fields); err != nil {
		return datastore, err
	}
	if len(fields.Names) > 1 || len(fields.Types) > 1 || len(fields.DSDrivers) > 1 ||
		len(fields.TMDrivers) > 1 || len(fields.DiskTypes) > 1 || len(fields.Templates) > 1 ||
		(len(fields.Templates) == 1 && (len(fields.Templates[0].Types) > 1 || len(fields.Templates[0].DiskTypes) > 1)) {
		return datastore, fmt.Errorf("%w: datastore %d has repeated authority fields", ErrConflictingAttachmentEvidence, datastoreID)
	}
	if err := xml.Unmarshal(data, &datastore); err != nil {
		return datastore, err
	}
	return datastore, nil
}
