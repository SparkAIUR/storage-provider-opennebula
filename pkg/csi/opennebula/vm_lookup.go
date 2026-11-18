package opennebula

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/OpenNebula/one/src/oca/go/src/goca/parameters"
	vmSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
)

const (
	VMResolutionByName         = "by_name"
	VMResolutionPoolExactMatch = "pool_exact_match"
	VMResolutionNotFound       = "not_found"
	VMResolutionDuplicateMatch = "duplicate_match"
)

type VMNotFoundError struct {
	Name string
}

func (e *VMNotFoundError) Error() string {
	if e == nil {
		return "opennebula vm not found"
	}
	return fmt.Sprintf("opennebula vm %q not found", e.Name)
}

type VMDuplicateNameError struct {
	Name  string
	Match []int
}

func (e *VMDuplicateNameError) Error() string {
	if e == nil {
		return "multiple opennebula vms matched"
	}
	return fmt.Sprintf("multiple opennebula vms matched %q: %v", e.Name, e.Match)
}

type VMNameInventory map[string][]int

func (inventory VMNameInventory) Resolve(nodeName string) (int, error) {
	id, _, _, err := inventory.resolve(nodeName)
	return id, err
}

func (inventory VMNameInventory) resolve(nodeName string) (int, string, string, error) {
	name, path := nodeName, VMResolutionByName
	matches := inventory[nodeName]
	if len(matches) == 0 {
		path = VMResolutionPoolExactMatch
		for candidate, ids := range inventory {
			if strings.EqualFold(candidate, nodeName) {
				name = candidate
				matches = append(matches, ids...)
			}
		}
	}
	id, err := uniqueVMMatch(nodeName, matches)
	var duplicate *VMDuplicateNameError
	switch {
	case errors.As(err, &duplicate):
		return -1, "", VMResolutionDuplicateMatch, err
	case err != nil:
		return -1, "", VMResolutionNotFound, err
	default:
		return id, name, path, nil
	}
}

func ResolveNodeVMID(ctx context.Context, ctrl *goca.Controller, nodeName string) (int, string, error) {
	id, _, path, err := resolveNodeVM(ctx, ctrl, nodeName)
	return id, path, err
}

func resolveNodeVM(ctx context.Context, ctrl *goca.Controller, nodeName string) (int, string, string, error) {
	if ctrl == nil {
		return -1, "", VMResolutionNotFound, errors.New("opennebula controller is nil")
	}
	observations, err := readVMPool(ctx, ctrl, false)
	if err != nil {
		return -1, "", VMResolutionNotFound, fmt.Errorf("failed to list OpenNebula VMs: %w", err)
	}
	return resolveVMRows(observations, nodeName)
}

func resolveVMRows(observations []vmAuthorityXML, nodeName string) (int, string, string, error) {
	inventory := make(VMNameInventory, len(observations))
	counts := vmIdentityCounts(observations)
	for _, observation := range observations {
		info, err := observation.vm(0)
		if err != nil {
			for _, name := range observation.Names {
				if strings.EqualFold(name, nodeName) {
					return -1, "", VMResolutionNotFound, err
				}
			}
			continue
		}
		inventory[info.Name] = append(inventory[info.Name], info.ID)
	}
	id, name, path, err := inventory.resolve(nodeName)
	if err == nil && counts[id] > 1 {
		return -1, "", VMResolutionNotFound, fmt.Errorf("%w: VM %d occurs under more than one identity", ErrConflictingAttachmentEvidence, id)
	}
	return id, name, path, err
}

type vmEvidence uint8

const (
	vmDisks vmEvidence = 1 << iota
	vmState
	vmHistory
)

type vmAuthorityXML struct {
	XMLName   xml.Name            `xml:"VM"`
	IDs       []string            `xml:"ID"`
	Names     []string            `xml:"NAME"`
	States    []string            `xml:"STATE"`
	LCMStates []string            `xml:"LCM_STATE"`
	Templates []vmSchema.Template `xml:"TEMPLATE"`
}

func (raw vmAuthorityXML) vm(required vmEvidence) (vmSchema.VM, error) {
	var info vmSchema.VM
	for _, field := range []struct {
		name     string
		values   []string
		dest     *int
		required bool
	}{
		{"ID", raw.IDs, &info.ID, true},
		{"STATE", raw.States, &info.StateRaw, required&vmState != 0},
		{"LCM_STATE", raw.LCMStates, &info.LCMStateRaw, required&vmState != 0},
	} {
		if len(field.values) > 1 || (field.required && len(field.values) == 0) {
			return info, fmt.Errorf("%w: invalid multiplicity of VM %s", ErrConflictingAttachmentEvidence, field.name)
		}
		if len(field.values) == 1 {
			value, err := strconv.Atoi(strings.TrimSpace(field.values[0]))
			if err != nil || value < 0 {
				return info, fmt.Errorf("%w: invalid VM %s", ErrConflictingAttachmentEvidence, field.name)
			}
			*field.dest = value
		}
	}
	if len(raw.Names) != 1 || strings.TrimSpace(raw.Names[0]) == "" {
		return info, fmt.Errorf("%w: invalid VM NAME", ErrConflictingAttachmentEvidence)
	}
	info.Name = raw.Names[0]
	if len(raw.Templates) > 1 || (required&vmDisks != 0 && len(raw.Templates) == 0) {
		return info, fmt.Errorf("%w: invalid multiplicity of VM TEMPLATE", ErrConflictingAttachmentEvidence)
	}
	if len(raw.Templates) == 1 {
		info.Template = raw.Templates[0]
	}
	return info, nil
}

func (raw vmAuthorityXML) candidateIDs() []int {
	var ids []int
	for _, value := range raw.IDs {
		id, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil && id >= 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func vmIdentityCounts(rows []vmAuthorityXML) map[int]int {
	counts := make(map[int]int, len(rows))
	for _, row := range rows {
		for _, id := range row.candidateIDs() {
			counts[id]++
		}
	}
	return counts
}

func (raw vmAuthorityXML) attachmentImages() ([]int, bool) {
	var ids []int
	for _, template := range raw.Templates {
		for _, disk := range template.GetDisks() {
			id, present, err := diskImageReference(disk, false)
			if err != nil || (present && !diskFieldsBelongToImage(disk, id)) {
				return nil, false
			}
			if present {
				ids = append(ids, id)
			}
		}
	}
	return ids, len(raw.Templates) != 0
}

func readVMPool(ctx context.Context, ctrl *goca.Controller, extended bool) ([]vmAuthorityXML, error) {
	read := ctrl.Client.VMPoolInfo
	if extended {
		read = ctrl.Client.VMPoolInfoExtended
	}
	response, err := read(ctx, parameters.PoolWhoAll, -1, -1, -1, "")
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	var pool struct {
		XMLName xml.Name         `xml:"VM_POOL"`
		VMs     []vmAuthorityXML `xml:"VM"`
	}
	err = xml.Unmarshal([]byte(response.Body()), &pool)
	return pool.VMs, err
}

type attachmentVMIdentity struct {
	id         int
	name       string
	lookupName string
}

func (p *PersistentDiskVolumeProvider) readAttachmentVMs(ctx context.Context, imageID int, requested attachmentVMIdentity, vmIDs ...int) ([]vmSchema.VM, error) {
	rows, err := readVMPool(ctx, p.ctrl, true)
	if err != nil {
		return nil, err
	}
	if requested.name != "" {
		lookupName := requested.lookupName
		if lookupName == "" {
			lookupName = requested.name
		}
		id, name, _, err := resolveVMRows(rows, lookupName)
		if err != nil {
			return nil, fmt.Errorf("%w: requested VM identity is unverified in the attachment census: %v", ErrConflictingAttachmentEvidence, err)
		}
		if id != requested.id || name != requested.name {
			return nil, fmt.Errorf("%w: requested VM identity changed in the attachment census", ErrConflictingAttachmentEvidence)
		}
	}
	var vms []vmSchema.VM
	counts := vmIdentityCounts(rows)
	for _, row := range rows {
		info, err := row.vm(vmDisks)
		if err == nil && counts[info.ID] > 1 {
			err = fmt.Errorf("%w: VM %d occurs more than once", ErrConflictingAttachmentEvidence, info.ID)
		}
		if err == nil {
			vms = append(vms, info)
			continue
		}
		images, scoped := row.attachmentImages()
		if !scoped || slices.Contains(images, imageID) || slices.ContainsFunc(row.candidateIDs(), func(id int) bool {
			return id > 0 && (id == requested.id || slices.Contains(vmIDs, id))
		}) {
			return nil, err
		}
	}
	return vms, nil
}

func (p *PersistentDiskVolumeProvider) readVMInfo(ctx context.Context, vmID int, required vmEvidence) (*vmSchema.VM, error) {
	response, err := p.ctrl.Client.VMInfo(ctx, vmID, true)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	data := []byte(response.Body())
	var raw vmAuthorityXML
	if err := xml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	info, err := raw.vm(required)
	if err != nil {
		return nil, err
	}
	if info.ID != vmID {
		return nil, fmt.Errorf("%w: requested VM %d identity differs from response %d", ErrConflictingAttachmentEvidence, vmID, info.ID)
	}
	if required&vmHistory != 0 {
		record, err := decodeVMHistoryAuthority(data)
		if err != nil {
			return nil, err
		}
		info.HistoryRecords = []vmSchema.HistoryRecord{record}
	}
	return &info, nil
}

func decodeVMHistoryAuthority(data []byte) (vmSchema.HistoryRecord, error) {
	type row struct {
		Seq       []string `xml:"SEQ"`
		Datastore []string `xml:"DS_ID"`
		Host      []string `xml:"HID"`
		Hostname  []string `xml:"HOSTNAME"`
	}
	var history struct {
		Groups []struct {
			Rows []row `xml:"HISTORY"`
		} `xml:"HISTORY_RECORDS"`
	}
	var record vmSchema.HistoryRecord
	conflict := fmt.Errorf("%w: VM system datastore history is unverified", ErrConflictingAttachmentEvidence)
	if err := xml.Unmarshal(data, &history); err != nil || len(history.Groups) != 1 {
		return record, conflict
	}
	number := func(fields []string) (int, bool) {
		if len(fields) != 1 {
			return 0, false
		}
		id, err := strconv.Atoi(strings.TrimSpace(fields[0]))
		return id, err == nil && id >= 0
	}
	maxSeq := -1
	var latest row
	unique := false
	for _, candidate := range history.Groups[0].Rows {
		seq, ok := number(candidate.Seq)
		if !ok {
			return record, conflict
		}
		switch {
		case seq > maxSeq:
			latest, maxSeq, unique = candidate, seq, true
		case seq == maxSeq:
			unique = false
		}
	}
	datastoreID, ok := number(latest.Datastore)
	if !unique || !ok || len(latest.Hostname) > 1 {
		return record, conflict
	}
	record.SEQ, record.DSID = maxSeq, datastoreID
	if len(latest.Host) != 0 {
		hostID, ok := number(latest.Host)
		if !ok {
			return record, conflict
		}
		record.HID = hostID
	}
	if len(latest.Hostname) == 1 {
		record.Hostname = latest.Hostname[0]
	}
	return record, nil
}

func verifyVMName(info *vmSchema.VM, expected string) error {
	if info.Name != expected {
		return fmt.Errorf("%w: VM %d name differs across observations", ErrConflictingAttachmentEvidence, info.ID)
	}
	return nil
}

func uniqueVMMatch(nodeName string, matches []int) (int, error) {
	switch len(matches) {
	case 0:
		return -1, &VMNotFoundError{Name: nodeName}
	case 1:
		return matches[0], nil
	default:
		return -1, &VMDuplicateNameError{Name: nodeName, Match: matches}
	}
}
