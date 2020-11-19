package kt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThresholdFilterTermUnmarshal(t *testing.T) {
	s := `
{
   "is_include":true,
   "entry_sets":[
      {
         "entries":[
            {
               "dimension":"i_device_label",
               "value":161
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      },
      {
         "entries":[
            {
               "dimension":"i_device_label",
               "value":172
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      },
      {
         "entries":[
            {
               "dimension":"i_device_site_name",
               "value":4
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      },
      {
         "entries":[
            {
               "dimension":"i_device_type",
               "value":"host-nprobe-dns-www"
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      },
      {
         "entries":[
            {
               "dimension":"i_device_name",
               "value":"new_device"
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      },
      {
         "entries":[
            {
               "dimension":"i_device_name",
               "value":"mnia_test2"
            },
            {
               "dimension":"i_input_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_output_interface_description",
               "value":"name"
            },
            {
               "dimension":"i_input_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"i_output_snmp_alias",
               "value":"desc"
            },
            {
               "dimension":"IP_src, IP_dst",
               "value":[
                  "127.0.0.1",
                  "192.168.0.1"
               ]
            },
            {
               "dimension":"AS_src",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            },
            {
               "dimension":"AS_dst",
               "value":[
                  "1",
                  "2",
                  "3"
               ]
            }
         ]
      }
   ]
}`
	var tf ThresholdFilter
	assert.NoError(t, json.Unmarshal([]byte(s), &tf))
	t.Logf("%+v", tf)

	// Noops still parse.
	assert.NoError(t, json.Unmarshal([]byte(`{}`), &tf))
	assert.NoError(t, json.Unmarshal([]byte(`{"is_include":true,"entry_sets":[{"entries":[]}]}`), &tf))
	assert.NoError(t, json.Unmarshal([]byte(`{"is_include":true,"entry_sets":[]}`), &tf))
	assert.NoError(t, json.Unmarshal([]byte(`{"entry_sets": [{"entries":[]}]}`), &tf))
}

func TestCompanyIDWithShadowBit(t *testing.T) {
	assert.Equal(t, Cid(1013), CompanyIDWithShadowBit(1013, false))
	assert.Equal(t, Cid(1013)+Cid(1<<30), CompanyIDWithShadowBit(1013, true))

	assert.Equal(t, Cid(1013), CompanyIDWithShadowBit(1013+1<<30, false))
	assert.Equal(t, Cid(1013)+Cid(1<<30), CompanyIDWithShadowBit(1013+1<<30, true))
}

func TestCompanyIDWithoutShadowBit(t *testing.T) {
	tests := []struct {
		name string
		cid  Cid
		want Cid
	}{
		{name: "Shadow policy, bit gets stripped", cid: 1073777481, want: 35657},
		{name: "Regular policy, nothing changes", cid: 35657, want: 35657},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompanyIDWithoutShadowBit(tt.cid); got != tt.want {
				t.Errorf("CompanyIDWithoutShadowBit() = %v, want %v", got, tt.want)
			}
		})
	}
}
