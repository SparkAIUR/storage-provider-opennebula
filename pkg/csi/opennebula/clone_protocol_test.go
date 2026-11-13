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
	for _, grownSourceSize := range []int{128, 256} {
		t.Run(fmt.Sprint(grownSourceSize), func(t *testing.T) {
			// OpenNebula atomically copies source TEMPLATE during clone. No transient CLONING_ID is used.
			self := map[int]int{7: 7}
			sizes := map[int]int{7: 128}
			parent := map[int]int{}
			names := map[int]string{7: "source"}
			var args []fixtureParam
			mutations := 0
			next := 8
			lost := true
			imageXML := func(id int) string {
				return fmt.Sprintf(`<IMAGE><ID>%d</ID><NAME>%s</NAME><STATE>1</STATE><SIZE>%d</SIZE><DATASTORE_ID>100</DATASTORE_ID><TEMPLATE><OWNER>csi.opennebula.io</OWNER><CSI_IMAGE_ID>%d</CSI_IMAGE_ID><CSI_CLONE_SOURCE_IMAGE_ID>%d</CSI_CLONE_SOURCE_IMAGE_ID></TEMPLATE></IMAGE>`, id, names[id], sizes[id], self[id], parent[id])
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
					sizes[id] = sizes[src]
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
					require.Equal(t, 1, args[3].number())
					update := map[string]string{}
					for _, line := range strings.Split(strings.TrimSpace(args[2].String), "\n") {
						key, raw, ok := strings.Cut(line, "=")
						require.True(t, ok)
						value, err := strconv.Unquote(strings.TrimSpace(raw))
						require.NoError(t, err)
						update[strings.TrimSpace(key)] = value
					}
					expected := map[int]map[string]string{
						8: {"CSI_IMAGE_ID": "8", "CSI_CLONE_SOURCE_IMAGE_ID": "7"},
						9: {"CSI_IMAGE_ID": "9", "CSI_CLONE_SOURCE_IMAGE_ID": "8"},
					}
					require.Equal(t, expected[id], update)
					if value, ok := update["CSI_IMAGE_ID"]; ok {
						self[id], _ = strconv.Atoi(value)
					}
					if value, ok := update["CSI_CLONE_SOURCE_IMAGE_ID"]; ok {
						parent[id], _ = strconv.Atoi(value)
					}
					return true, strconv.Itoa(id), 0
				default:
					t.Errorf("unexpected method %s", method)
					return false, "unexpected", 0x800
				}
			}, func(_ string, raw []byte) { args = fixtureArgs(t, raw) })
			selection := DatastoreSelectionConfig{Identifiers: []string{"100"}, AllowedTypes: []string{"local"}, CloneLimitBytes: 128 * sizeConversion}
			_, err := p.CloneVolume(context.Background(), "child", "source", selection)
			require.ErrorContains(t, err, "acknowledgement lost")
			require.Equal(t, 1, mutations)
			// A new provider instance has no in-memory operation state.
			sizes[7] = grownSourceSize
			restarted := *p
			result, err := restarted.CloneVolume(context.Background(), "child", "source", selection)
			require.NoError(t, err)
			require.EqualValues(t, 128*sizeConversion, result.CapacityBytes)
			require.Equal(t, 1, mutations)
			require.Equal(t, 7, parent[8])
			require.Equal(t, 8, self[8])
			if grownSourceSize == 256 {
				_, err = restarted.CloneVolume(context.Background(), "new-child", "source", selection)
				require.ErrorIs(t, err, ErrCloneCapacity)
				require.Equal(t, 1, mutations)
			}
			for _, invalidSize := range []int{64, 256} {
				sizes[8] = invalidSize
				bounded := selection
				bounded.CloneRequiredBytes = 128 * sizeConversion
				_, err = restarted.CloneVolume(context.Background(), "child", "source", bounded)
				require.ErrorIs(t, err, ErrCloneCapacity)
				require.Equal(t, 1, mutations)
			}
			sizes[8] = 128
			_, err = restarted.CloneVolume(context.Background(), "grandchild", "child", selection)
			require.NoError(t, err)
			require.Equal(t, 2, mutations)
			require.Equal(t, 8, parent[9])
			require.Equal(t, 9, self[9])
		})
	}
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

func TestCloneFreshTargetCapacityReadback(t *testing.T) {
	for _, capacity := range []int{64, 128, 256} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			cloned := false
			source := `<IMAGE><ID>7</ID><NAME>source</NAME><STATE>1</STATE><SIZE>128</SIZE><DATASTORE_ID>100</DATASTORE_ID><TEMPLATE><OWNER>csi.opennebula.io</OWNER><CSI_IMAGE_ID>7</CSI_IMAGE_ID></TEMPLATE></IMAGE>`
			target := fmt.Sprintf(`<IMAGE><ID>8</ID><NAME>child</NAME><STATE>1</STATE><SIZE>%d</SIZE><DATASTORE_ID>100</DATASTORE_ID><TEMPLATE><OWNER>csi.opennebula.io</OWNER><CSI_IMAGE_ID>8</CSI_IMAGE_ID><CSI_CLONE_SOURCE_IMAGE_ID>7</CSI_CLONE_SOURCE_IMAGE_ID></TEMPLATE></IMAGE>`, capacity)
			var args []fixtureParam
			p := lifecycleFixture(t, func(method string) (bool, string, int) {
				switch method {
				case "one.imagepool.info":
					body := source
					if cloned {
						body += target
					}
					return true, "<IMAGE_POOL>" + body + "</IMAGE_POOL>", 0
				case "one.image.info":
					if args[1].number() == 7 {
						return true, source, 0
					}
					return true, target, 0
				case "one.datastorepool.info":
					return true, `<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>default</NAME><TYPE>IMAGE</TYPE><DS_MAD>fs</DS_MAD><TM_MAD>local</TM_MAD><STATE>0</STATE><FREE_MB>10000</FREE_MB><TOTAL_MB>20000</TOTAL_MB></DATASTORE></DATASTORE_POOL>`, 0
				case "one.image.clone":
					cloned = true
					return true, "8", 0
				default:
					t.Errorf("unexpected method %s", method)
					return false, "unexpected", 0x800
				}
			}, func(_ string, raw []byte) { args = fixtureArgs(t, raw) })
			selection := DatastoreSelectionConfig{Identifiers: []string{"100"}, AllowedTypes: []string{"local"}, CloneRequiredBytes: 128 * sizeConversion, CloneLimitBytes: 128 * sizeConversion}
			result, err := p.CloneVolume(context.Background(), "child", "source", selection)
			if capacity == 128 {
				require.NoError(t, err)
				require.EqualValues(t, 128*sizeConversion, result.CapacityBytes)
			} else {
				require.ErrorIs(t, err, ErrCloneCapacity)
				require.Nil(t, result)
			}
			require.True(t, cloned)
		})
	}
}
