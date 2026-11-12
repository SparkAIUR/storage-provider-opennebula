package opennebula

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"testing"

	img "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/image"
	"github.com/stretchr/testify/require"
)

type fixtureParam struct {
	Int    int    `xml:"int"`
	I4     int    `xml:"i4"`
	I8     int    `xml:"i8"`
	String string `xml:"string"`
}

func fixtureArgs(t *testing.T, raw []byte) []fixtureParam {
	t.Helper()
	var call struct {
		Params []struct {
			Value fixtureParam `xml:"value"`
		} `xml:"params>param"`
	}
	require.NoError(t, xml.Unmarshal(raw, &call))
	out := []fixtureParam{}
	for _, p := range call.Params {
		out = append(out, p.Value)
	}
	return out
}
func (v fixtureParam) number() int { return v.Int + v.I4 + v.I8 }

func TestCloneLostAcknowledgementRestartAndCloneOfClone(t *testing.T) {
	// OpenNebula atomically copies source TEMPLATE during clone. No transient CLONING_ID is used.
	self := map[int]int{7: 7}
	parent := map[int]int{}
	names := map[int]string{7: "source"}
	var args []fixtureParam
	mutations := 0
	next := 8
	lost := true
	imageXML := func(id int) string {
		return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>1</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID><TEMPLATE><OWNER>csi.opennebula.io</OWNER><CSI_IMAGE_ID>%d</CSI_IMAGE_ID><CSI_CLONE_SOURCE_IMAGE_ID>%d</CSI_CLONE_SOURCE_IMAGE_ID></TEMPLATE></IMAGE>`, id, names[id], self[id], parent[id])
	}
	// Missing parent must be absent, not the invalid value zero.
	imageXMLValid := func(id int) string {
		return strings.ReplaceAll(imageXML(id), "<CSI_CLONE_SOURCE_IMAGE_ID>0</CSI_CLONE_SOURCE_IMAGE_ID>", "")
	}
	p := lifecycleFixture(t, func(method string) (bool, string, int) {
		switch method {
		case "one.imagepool.info":
			body := "<IMAGE_POOL>"
			for id := 7; id < next; id++ {
				body += imageXMLValid(id)
			}
			return true, body + "</IMAGE_POOL>", 0
		case "one.image.info":
			return true, imageXMLValid(args[1].number()), 0
		case "one.datastorepool.info":
			return true, `<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>default</NAME><TYPE>IMAGE</TYPE><DS_MAD>fs</DS_MAD><TM_MAD>local</TM_MAD><STATE>0</STATE><FREE_MB>10000</FREE_MB><TOTAL_MB>20000</TOTAL_MB></DATASTORE></DATASTORE_POOL>`, 0
		case "one.image.clone":
			require.Equal(t, 100, args[3].number())
			src := args[1].number()
			id := next
			next++
			self[id] = self[src]
			parent[id] = parent[src]
			names[id] = args[2].String
			mutations++
			if lost {
				lost = false
				return false, "acknowledgement lost", 0x800
			}
			return true, strconv.Itoa(id), 0
		case "one.image.update":
			id := args[1].number()
			tpl := args[2].String
			require.Contains(t, tpl, fmt.Sprintf(`CSI_IMAGE_ID="%d"`, id))
			// Updates normalize self and inherited origin in the same mutation.
			if self[id] != id {
				parent[id] = self[id]
			}
			self[id] = id
			return true, strconv.Itoa(id), 0
		default:
			t.Errorf("unexpected method %s", method)
			return false, "unexpected", 0x800
		}
	}, func(_ string, raw []byte) { args = fixtureArgs(t, raw) })
	selection := DatastoreSelectionConfig{Identifiers: []string{"100"}, AllowedTypes: []string{"local"}}
	_, err := p.CloneVolume(context.Background(), "child", "source", selection)
	require.ErrorContains(t, err, "acknowledgement lost")
	require.Equal(t, 1, mutations)
	// A new provider instance has no in-memory operation state.
	restarted := *p
	result, err := restarted.CloneVolume(context.Background(), "child", "source", selection)
	require.NoError(t, err)
	require.EqualValues(t, 128*sizeConversion, result.CapacityBytes)
	require.Equal(t, 1, mutations)
	require.Equal(t, 7, parent[8])
	require.Equal(t, 8, self[8])
	_, err = restarted.CloneVolume(context.Background(), "grandchild", "child", selection)
	require.NoError(t, err)
	require.Equal(t, 2, mutations)
	require.Equal(t, 8, parent[9])
	require.Equal(t, 9, self[9])
}

func TestCloneProvenanceRejectsNameOnlyWithoutMutation(t *testing.T) {
	mutations := 0
	p := lifecycleFixture(t, func(string) (bool, string, int) { mutations++; return false, "unexpected", 0x800 })
	var target img.Image
	require.NoError(t, xml.Unmarshal([]byte(`<IMAGE><ID>8</ID><NAME>child</NAME><TEMPLATE><OWNER>csi.opennebula.io</OWNER></TEMPLATE></IMAGE>`), &target))
	_, err := p.verifiedCloneTarget(context.Background(), &target, 7)
	require.ErrorIs(t, err, ErrCloneConflict)
	require.Zero(t, mutations)
}
