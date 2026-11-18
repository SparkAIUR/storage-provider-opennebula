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
	matches := inventory[nodeName]
	if len(matches) == 0 {
		for name, ids := range inventory {
			if strings.EqualFold(name, nodeName) {
				matches = append(matches, ids...)
			}
		}
	}
	return uniqueVMMatch(nodeName, matches)
}

func ResolveNodeVMID(ctx context.Context, ctrl *goca.Controller, nodeName string) (int, string, error) {
	if ctrl == nil {
		return -1, VMResolutionNotFound, errors.New("opennebula controller is nil")
	}
	observations, err := readVMAuthorityPool(ctx, ctrl)
	if err != nil {
		return -1, VMResolutionNotFound, fmt.Errorf("failed to list OpenNebula VMs: %w", err)
	}
	var vms []vmSchema.VM
	invalid := make(map[int]error)
	for _, observation := range observations {
		info, err := observation.vm()
		if err != nil {
			for _, name := range observation.Names {
				if strings.EqualFold(name, nodeName) {
					return -1, VMResolutionNotFound, err
				}
			}
			for _, id := range observation.candidateIDs() {
				invalid[id] = err
			}
			continue
		}
		vms = append(vms, info)
	}
	id, path, err := resolveNodeVMIDWithFallback(
		nodeName,
		func(name string) (int, error) {
			var matches []int
			for _, candidate := range vms {
				if candidate.Name == name {
					matches = append(matches, candidate.ID)
				}
			}
			return uniqueVMMatch(name, matches)
		},
		func() ([]vmSchema.VM, error) {
			return vms, nil
		},
	)
	if err == nil && invalid[id] != nil {
		return -1, VMResolutionNotFound, invalid[id]
	}
	return id, path, err
}

type vmAuthorityXML struct {
	XMLName   xml.Name            `xml:"VM"`
	IDs       []string            `xml:"ID"`
	Names     []string            `xml:"NAME"`
	States    []string            `xml:"STATE"`
	LCMStates []string            `xml:"LCM_STATE"`
	Templates []vmSchema.Template `xml:"TEMPLATE"`
}

func (raw vmAuthorityXML) vm() (vmSchema.VM, error) {
	var info vmSchema.VM
	for _, field := range []struct {
		name   string
		values []string
		dest   *int
	}{
		{"ID", raw.IDs, &info.ID}, {"STATE", raw.States, &info.StateRaw}, {"LCM_STATE", raw.LCMStates, &info.LCMStateRaw},
	} {
		if len(field.values) > 1 || (field.name == "ID" && len(field.values) == 0) {
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
	if len(raw.Names) > 1 || len(raw.Templates) > 1 {
		return info, fmt.Errorf("%w: repeated VM NAME or TEMPLATE", ErrConflictingAttachmentEvidence)
	}
	if len(raw.Names) == 1 {
		info.Name = raw.Names[0]
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

func readVMAuthorityPool(ctx context.Context, ctrl *goca.Controller) ([]vmAuthorityXML, error) {
	response, err := ctrl.Client.VMPoolInfo(ctx, parameters.PoolWhoAll, -1, -1, -1, "")
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

func (p *PersistentDiskVolumeProvider) readAttachmentVMs(ctx context.Context, imageID int, vmIDs ...int) ([]vmSchema.VM, error) {
	rows, err := readVMAuthorityPool(ctx, p.ctrl)
	if err != nil {
		return nil, err
	}
	var vms []vmSchema.VM
	for _, row := range rows {
		info, err := row.vm()
		if err == nil {
			vms = append(vms, info)
			continue
		}
		images, scoped := row.attachmentImages()
		if !scoped || slices.Contains(images, imageID) || slices.ContainsFunc(row.candidateIDs(), func(id int) bool {
			return id > 0 && slices.Contains(vmIDs, id)
		}) {
			return nil, err
		}
	}
	return vms, nil
}

func (p *PersistentDiskVolumeProvider) readVMInfo(ctx context.Context, vmID int) (*vmSchema.VM, error) {
	response, err := p.ctrl.Client.VMInfo(ctx, vmID, true)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if err != nil {
		return nil, err
	}
	var raw vmAuthorityXML
	if err := xml.Unmarshal([]byte(response.Body()), &raw); err != nil {
		return nil, err
	}
	info, err := raw.vm()
	if err == nil && info.ID != vmID {
		err = fmt.Errorf("%w: requested VM %d identity differs from response %d", ErrConflictingAttachmentEvidence, vmID, info.ID)
	}
	return &info, err
}

func resolveNodeVMIDWithFallback(nodeName string, byName func(string) (int, error), listVMs func() ([]vmSchema.VM, error)) (int, string, error) {
	if id, err := byName(nodeName); err == nil {
		return id, VMResolutionByName, nil
	}

	vms, err := listVMs()
	if err != nil {
		return -1, VMResolutionNotFound, fmt.Errorf("failed to list OpenNebula VMs: %w", err)
	}

	matches := make([]int, 0, 1)
	for _, candidate := range vms {
		if strings.EqualFold(candidate.Name, nodeName) {
			matches = append(matches, candidate.ID)
		}
	}

	id, err := uniqueVMMatch(nodeName, matches)
	var duplicate *VMDuplicateNameError
	if errors.As(err, &duplicate) {
		return -1, VMResolutionDuplicateMatch, err
	}
	if err != nil {
		return -1, VMResolutionNotFound, err
	}
	return id, VMResolutionPoolExactMatch, nil
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
