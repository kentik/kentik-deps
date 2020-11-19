package kt

import (
	"strings"
)

// TODO(tjonak): Golang warning about camelcase
const (
	DST_AS                     = "dst_as"
	DST_GEO                    = "dst_geo"
	DST_MAC                    = "dst_mac"
	DST_ETH_MAC                = "dst_eth_mac"
	HEADERLEN                  = "headerlen"
	IN_BYTES                   = "in_bytes"
	IN_PKTS                    = "in_pkts"
	BOTH_BYTES                 = "both_bytes"
	BOTH_PKTS                  = "both_pkts"
	FLOWS                      = "flows"
	OUT_BYTES                  = "out_bytes"
	OUT_PKTS                   = "out_pkts"
	INPUT_PORT                 = "input_port"
	IPSIZE                     = "ipsize"
	IPV4_DST_ADDR              = "ipv4_dst_addr"
	IPV4_SRC_ADDR              = "ipv4_src_addr"
	INET_DST_ADDR              = "inet_dst_addr"
	INET_SRC_ADDR              = "inet_src_addr"
	L4_DST_PORT                = "l4_dst_port"
	L4_SRC_PORT                = "l4_src_port"
	OUTPUT_PORT                = "output_port"
	PROTOCOL                   = "protocol"
	PROTOCOL_NAME              = "i_protocol_name"
	SRC_AS                     = "src_as"
	SRC_GEO                    = "src_geo"
	SRC_MAC                    = "src_mac"
	SRC_ETH_MAC                = "src_eth_mac"
	TCP_FLAGS                  = "tcp_flags"
	TCP_FLAGS_RAW              = "tcp_flags_raw"
	TOS                        = "tos"
	VLAN_IN                    = "vlan_in"
	VLAN_OUT                   = "vlan_out"
	IPV4_SRC_NEXT_HOP          = "ipv4_src_next_hop"
	IPV4_DST_NEXT_HOP          = "ipv4_dst_next_hop"
	INET_SRC_NEXT_HOP          = "inet_src_next_hop"
	INET_DST_NEXT_HOP          = "inet_dst_next_hop"
	MPLS_TYPE                  = "mpls_type"
	TCP_RETRANSMIT             = "tcp_retransmit"
	SAMPLE_RATE                = "sample_rate"
	DEVICE_ID                  = "device_id"
	I_DEVICE_ID                = "i_device_id"
	DEVICE_TYPE                = "i_device_type"
	DEVICE_NAME                = "device_name"
	I_DEVICE_NAME              = "i_device_name"
	DEVICE_SITE_NAME           = "device_site_name"
	I_DEVICE_SITE_NAME         = "i_device_site_name"
	DEVICE_SITE_COUNTRY        = "i_device_site_country"
	SRC_FLOW_TAGS              = "src_flow_tags"
	DST_FLOW_TAGS              = "dst_flow_tags"
	SRC_GEO_REGION             = "src_geo_region"
	DST_GEO_REGION             = "dst_geo_region"
	SRC_GEO_CITY               = "src_geo_city"
	DST_GEO_CITY               = "dst_geo_city"
	DST_BGP_ASPATH             = "dst_bgp_aspath"
	DST_BGP_COMMUNITY          = "dst_bgp_community"
	SRC_BGP_ASPATH             = "src_bgp_aspath"
	SRC_BGP_COMMUNITY          = "src_bgp_community"
	SRC_CDN                    = "src_cdn"
	DST_CDN                    = "dst_cdn"
	ANY_CDN                    = "src|dst_cdn"
	SRC_AS_NAME                = "i_src_as_name"
	DST_AS_NAME                = "i_dst_as_name"
	SRC_SECOND_AS_NAME         = "i_src_second_asn_name"
	DST_SECOND_AS_NAME         = "i_dst_second_asn_name"
	SRC_THIRD_AS_NAME          = "i_src_third_asn_name"
	DST_THIRD_AS_NAME          = "i_dst_third_asn_name"
	SRC_NEXTHOP_NAME           = "i_src_nexthop_as_name"
	DST_NEXTHOP_NAME           = "i_dst_nexthop_as_name"
	INPUT_SNMP_ALIAS           = "i_input_snmp_alias"
	INPUT_SNMP_DESC            = "i_input_interface_description"
	OUTPUT_SNMP_ALIAS          = "i_output_snmp_alias"
	OUTPUT_SNMP_DESC           = "i_output_interface_description"
	IPV4_SRC_CIDR              = "ipv4_src_cidr"
	IPV4_DST_CIDR              = "ipv4_dst_cidr"
	IPV4_SRC_CIDR_NOT          = "ipv4_src_cidr_not"
	IPV4_DST_CIDR_NOT          = "ipv4_dst_cidr_not"
	IPV4_DST_NEXT_HOP_CIDR     = "ipv4_dst_next_hop_cidr"
	IPV4_SRC_NEXT_HOP_CIDR     = "ipv4_src_next_hop_cidr"
	IPV4_DST_NEXT_HOP_CIDR_NOT = "ipv4_dst_next_hop_cidr_not"
	IPV4_SRC_NEXT_HOP_CIDR_NOT = "ipv4_src_next_hop_cidr_not"
	IPV4_DST_ROUTE_PREFIX      = "ipv4_dst_route_prefix"
	IPV4_SRC_ROUTE_PREFIX      = "ipv4_src_route_prefix"
	INET_SRC_CIDR              = "inet_src_cidr"
	INET_DST_CIDR              = "inet_dst_cidr"
	INET_SRC_CIDR_NOT          = "inet_src_cidr_not"
	INET_DST_CIDR_NOT          = "inet_dst_cidr_not"
	INET_DST_NEXT_HOP_CIDR     = "inet_dst_next_hop_cidr"
	INET_SRC_NEXT_HOP_CIDR     = "inet_src_next_hop_cidr"
	INET_DST_NEXT_HOP_CIDR_NOT = "inet_dst_next_hop_cidr_not"
	INET_SRC_NEXT_HOP_CIDR_NOT = "inet_src_next_hop_cidr_not"
	INET_DST_ROUTE_PREFIX      = "inet_dst_route_prefix"
	INET_SRC_ROUTE_PREFIX      = "inet_src_route_prefix"
	SRC_SECOND_AS              = "src_second_asn"
	DST_SECOND_AS              = "dst_second_asn"
	SRC_THIRD_AS               = "src_third_asn"
	DST_THIRD_AS               = "dst_third_asn"
	SRC_NEXTHOP                = "src_nexthop_as"
	DST_NEXTHOP                = "dst_nexthop_as"
	INET_FAMILY                = "inet_family"
	DST_ROUTE_LENGTH           = "dst_route_length"
	SRC_ROUTE_LENGTH           = "src_route_length"
	PACKETSIZE                 = "sampledpktsize"
	PACKETSIZE_100             = "sampledpktsize_100"
	MEASURE_SEPERATOR          = ","
	ANY_COUNTRY                = "src|dst_geo"
	ANY_AS_NUMBER              = "src|dst_as"
	ANY_AS_NAME                = "i_src|dst_as_name"
	ANY_FLOW_TAG               = "src|dst_flow_tags"
	ANY_L4_PORT                = "l4_src|dst_port"
	ANY_MAC_ADDR               = "src|dst_eth_mac"
	ANY_IP_ADDR                = "inet_src|dst_addr"
	ANY_PORT                   = "input|output_port"
	ANY_INTERFACE_DESC         = "i_input|output_interface_description"
	ANY_INTERFACE_ALIAS        = "i_input|output_snmp_alias"
	ANY_NETWORK_BNDRY          = "i_src|dst_network_bndry_name"
	ANY_ROUTE_PREFIX           = "inet_src|dst_route_prefix"
	ANY_PROVIDER               = "i_src|dst_provider_classification"
	ANY_CONNECT_TYPE           = "i_src|dst_connect_type_name"
	ANY_TRAF                   = "i_trf_origination|termination"
	INPUT_INTERFACE_SPEED      = "i_input_interface_speed"
	OUTPUT_INTERFACE_SPEED     = "i_output_interface_speed"
	OUTPUT_PROVIDER            = "i_dst_provider_classification"
	INPUT_PROVIDER             = "i_src_provider_classification"
	SRC_CONNECT_TYPE           = "i_src_connect_type_name"
	DST_CONNECT_TYPE           = "i_dst_connect_type_name"
	SRC_NETWORK_BNDRY          = "i_src_network_bndry_name"
	DST_NETWORK_BNDRY          = "i_dst_network_bndry_name"
	ULT_EXIT_NETWORK_BNDRY     = "i_ult_exit_network_bndry_name"
	ULT_EXIT_CONNECT_TYPE      = "i_ult_exit_connect_type_name"
	ULT_EXIT_SITE_COUNTRY      = "i_ult_exit_site_country"
	ULT_EXIT_SITE              = "i_ult_exit_site"
	ULT_EXIT_INTERFACE_DESC    = "i_ult_exit_interface_description"
	ULT_EXIT_SNMP_ALIAS        = "i_ult_exit_snmp_alias"
	ULT_EXIT_PROVIDER          = "i_ult_provider_classification"
	TRAF_ORIG                  = "i_trf_origination"
	TRAF_TERM                  = "i_trf_termination"
	HOST_DIR                   = "i_host_direction"
	TRAF_PROF                  = "i_trf_profile"
	DEVICE_LABEL               = "i_device_label"
	SRC_THREAT_HOST            = "src_threat_host"
	DST_THREAT_HOST            = "dst_threat_host"
	ANY_THREAT_HOST            = "src|dst_threat_host"
	SRC_THREAT_BNETCC          = "src_threat_bnetcc"
	DST_THREAT_BNETCC          = "dst_threat_bnetcc"
	ANY_THREAT_BNETCC          = "src|dst_threat_bnetcc"
	FIREEYE_SRC                = "c_fireeye_src"
	FIREEYE_DST                = "c_fireeye_dst"

	InputInterfaceGroup  = "i_input_interface_group"
	OutputInterfaceGroup = "i_output_interface_group"
	AnyInterfaceGroup    = "i_input|output_interface_group"

	SynColumnName = "synthetic"

	InputVRF     = "input_vrf"
	InputVRFName = "i_input_vrf"
	InputVRFRd   = "i_input_vrf_rd"
	InputVRFRt   = "i_input_vrf_rt"

	OutputVRF     = "output_vrf"
	OutputVRFName = "i_output_vrf"
	OutputVRFRd   = "i_output_vrf_rd"
	OutputVRFRt   = "i_output_vrf_rt"

	AnyVRFName  = "i_input|output_vrf"
	AnyVRFRd    = "i_input|output_vrf_rd"
	AnyVRFRt    = "i_input|output_vrf_rt"
	AnyVRFExtRd = "input|output_vrf"

	DeviceSubtype = "i_device_subtype"

	MEASURE_BITS                        = "bits"
	MEASURE_PACKETS                     = "packets"
	MEASURE_FLOWS                       = "fps"
	MEASURE_UNIQUE_SRC_IP               = "unique_src_ip"
	MEASURE_UNIQUE_DST_IP               = "unique_dst_ip"
	MEASURE_UNIQUE_SRC_PORT             = "unique_src_port"
	MEASURE_UNIQUE_DST_PORT             = "unique_dst_port"
	MEASURE_UNIQUE_SRC_AS               = "unique_src_as"
	MEASURE_UNIQUE_DST_AS               = "unique_dst_as"
	MEASURE_UNIQUE_SRC_GEO              = "unique_src_geo"
	MEASURE_UNIQUE_DST_GEO              = "unique_dst_geo"
	MEASURE_UNIQUE_SRC_CITY             = "unique_src_city"
	MEASURE_UNIQUE_DST_CITY             = "unique_dst_city"
	MEASURE_UNIQUE_SRC_REGION           = "unique_src_region"
	MEASURE_UNIQUE_DST_REGION           = "unique_dst_region"
	MEASURE_UNIQUE_DST_NEXTHOP_ASN      = "unique_dst_nexthop_asn"
	MEASURE_CUSTOM_RETRANSMITS          = "retransmits_out"
	MEASURE_CUSTOM_CLIENT_LATENCY       = "client_latency"
	MEASURE_CUSTOM_SERVER_LATENCY       = "server_latency"
	MEASURE_CUSTOM_APP_LATENCY          = "appl_latency"
	MEASURE_CUSTOM_OOO                  = "out_of_order_in"
	MEASURE_CUSTOM_FRAGMENTS            = "fragments"
	MEASURE_CUSTOM_FPEX_LATENCY_MS      = "fpex_latency_ms"
	MEASURE_CUSTOM_REPEATED_RETRANSMITS = "repeated_retransmits"
	MEASURE_CUSTOM_RETRANSMIT_PERC      = "perc_retransmits_in"
	MEASURE_CUSTOM_OOO_PERC             = "perc_out_of_order_in"
	MEASURE_CUSTOM_FRAGMENT_PERC        = "perc_fragments"

	MeasureTypeSum = "sum"
	MeasureTypeAvg = "avg"

	DIMENSION_GEO_SRC                   = "Geography_src"
	DIMENSION_SRC_GEO_REGION            = "src_geo_region"
	DIMENSION_SRC_GEO_CITY              = "src_geo_city"
	DIMENSION_AS_SRC                    = "AS_src"
	DIMENSION_PACKETSIZE_100            = "sampledpktsize_100"
	DIMENSION_PACKETSIZE                = "sampledpktsize"
	DIMENSION_INTERFACEID_SRC           = "InterfaceID_src"
	DIMENSION_PORT_SRC                  = "Port_src"
	DIMENSION_SRC_ETH_MAC               = "src_eth_mac"
	DIMENSION_VLAN_SRC                  = "VLAN_src"
	DIMENSION_IP_SRC                    = "IP_src"
	DIMENSION_IP_SRC_CIDR               = "IP_src_cidr_"
	DIMENSION_SRC_ROUTE_PREFIX          = "src_route_prefix"
	DIMENSION_SRC_ROUTE_LENGTH          = "src_route_length"
	DIMENSION_SRC_BGP_COMMUNITY         = "src_bgp_community"
	DIMENSION_SRC_BGP_ASPATH            = "src_bgp_aspath"
	DIMENSION_SRC_NEXTHOP_IP            = "src_nexthop_ip"
	DIMENSION_SRC_NEXTHOP_IP_CIDR       = "src_nexthop_ip_cidr_"
	DIMENSION_SRC_NEXTHOP_ASN           = "src_nexthop_asn"
	DIMENSION_SRC_SECOND_ASN            = "src_second_asn"
	DIMENSION_SRC_THIRD_ASN             = "src_third_asn"
	DIMENSION_SRC_PROTO_PORT            = "src_proto_port"
	DIMENSION_GEOGRAPHY_DST             = "Geography_dst"
	DIMENSION_DST_GEO_REGION            = "dst_geo_region"
	DIMENSION_DST_GEO_CITY              = "dst_geo_city"
	DIMENSION_AS_DST                    = "AS_dst"
	DIMENSION_INTERFACEID_DST           = "InterfaceID_dst"
	DIMENSION_PORT_DST                  = "Port_dst"
	DIMENSION_DST_ETH_MAC               = "dst_eth_mac"
	DIMENSION_VLAN_DST                  = "VLAN_dst"
	DIMENSION_IP_DST                    = "IP_dst"
	DIMENSION_IP_DST_CIDR               = "IP_dst_cidr_"
	DIMENSION_DST_ROUTE_PREFIX          = "dst_route_prefix"
	DIMENSION_DST_ROUTE_LENGTH          = "dst_route_length"
	DIMENSION_DST_BGP_COMMUNITY         = "dst_bgp_community"
	DIMENSION_DST_BGP_ASPATH            = "dst_bgp_aspath"
	DIMENSION_DST_NEXTHOP_IP            = "dst_nexthop_ip"
	DIMENSION_DST_NEXTHOP_IP_CIDR       = "dst_nexthop_ip_cidr_"
	DIMENSION_DST_NEXTHOP_ASN           = "dst_nexthop_asn"
	DIMENSION_DST_SECOND_ASN            = "dst_second_asn"
	DIMENSION_DST_THIRD_ASN             = "dst_third_asn"
	DIMENSION_DST_PROTO_PORT            = "dst_proto_port"
	DIMENSION_TRAFFIC                   = "Traffic"
	DIMENSION_TOPFLOW                   = "TopFlow"
	DIMENSION_PROTO                     = "Proto"
	DIMENSION_INTERFACETOPTALKERS       = "InterfaceTopTalkers"
	DIMENSION_PORTTOPTALKERS            = "PortTopTalkers"
	DIMENSION_REGIONTOPTALKERS          = "RegionTopTalkers"
	DIMENSION_TOPFLOWSIP                = "TopFlowsIP"
	DIMENSION_TOPFLOWSIP_CIDR           = "TopFlowsIP_cidr_"
	DIMENSION_ASTOPTALKERS              = "ASTopTalkers"
	DIMENSION_TOS                       = "TOS"
	DIMENSION_TCP_FLAGS                 = "tcp_flags"
	DIMENSION_DEVICE_ID                 = "device_id"
	DIMENSION_I_DEVICE_ID               = "i_device_id"
	DimensionDeviceName                 = "i_device_name"
	DIMENSION_SITE_NAME                 = "device_site_name"
	DIMENSION_I_SITE_NAME               = "i_device_site_name"
	DIMENSION_UNIQUE_SRC_IP             = "unique_src_ip"
	DIMENSION_UNIQUE_DST_IP             = "unique_dst_ip"
	DIMENSION_UNIQUE_SRC_AS             = "unique_src_as"
	DIMENSION_UNIQUE_DST_AS             = "unique_dst_as"
	DIMENSION_UNIQUE_SRC_GEO            = "unique_src_geo"
	DIMENSION_UNIQUE_DST_GEO            = "unique_dst_geo"
	DIMENSION_SRC_ROUTE_PREFIX_LEN      = "src_route_prefix_len"
	DIMENSION_DST_ROUTE_PREFIX_LEN      = "dst_route_prefix_len"
	DIMENSION_INET_SRC                  = "IP INET_src"
	DIMENSION_INET_DST                  = "IP_INET_dst"
	DIMENSION_INET_FAMILY               = "inet_family"
	DIMENSION_INTERFACE_OUTPUT_PROVIDER = "i_dst_provider_classification"
	DIMENSION_INTERFACE_INPUT_PROVIDER  = "i_src_provider_classification"
	DIMENSION_INPUT_INTERFACE_SPEED     = "i_input_interface_speed"
	DIMENSION_OUTPUT_INTERFACE_SPEED    = "i_output_interface_speed"
	DimensionSrcCdn                     = "src_cdn"
	DimensionDstCdn                     = "dst_cdn"
	DIMENSION_SRC_CONNECT_TYPE          = "i_src_connect_type_name"
	DIMENSION_DST_CONNECT_TYPE          = "i_dst_connect_type_name"
	DIMENSION_SRC_NETWORK_BNDRY         = "i_src_network_bndry_name"
	DIMENSION_DST_NETWORK_BNDRY         = "i_dst_network_bndry_name"
	DIMENSION_ULT_EXIT_NETWORK_BNDRY    = "i_ult_exit_network_bndry_name"
	DIMENSION_ULT_EXIT_CONNECT_TYPE     = "i_ult_exit_connect_type_name"
	DIMENSION_TRAF_ORIG                 = "i_trf_origination"
	DIMENSION_TRAF_TERM                 = "i_trf_termination"
	DIMENSION_HOST_DIR                  = "i_host_direction"
	DIMENSION_TRAF_PROF                 = "i_trf_profile"
	DimensionOutputPort                 = "output_port"
	DIMENSION_ULT_EXIT_DEVICE_NAME      = "i_ult_exit_device_name"
	DIMENSION_ULT_EXIT_SNMP_ALIAS       = "i_ult_exit_snmp_alias"
	DIMENSION_BGP_ULT_EXIT_INTERFACE    = "bgp_ult_exit_interface"

	DimensionInputVRF      = InputVRF
	DimensionInputVRFName  = InputVRFName
	DimensionInputVRFRd    = InputVRFRd
	DimensionInputVRFRt    = InputVRFRt
	DimensionOutputVRF     = OutputVRF
	DimensionOutputVRFName = OutputVRFName
	DimensionOutputVRFRd   = OutputVRFRd
	DimensionOutputVRFRt   = OutputVRFRt

	FORMAT_INT64              = "int64"
	FORMAT_UINT8              = "uint8"
	FORMAT_UINT16             = "uint16"
	FORMAT_UINT32             = "uint32"
	FORMAT_UINT64             = "uint64"
	FORMAT_STRING             = "string"
	FORMAT_FLOAT32            = "float32"
	FORMAT_BYTE               = "byte"
	FORMAT_USHORT             = "uint16"
	FORMAT_ADDR               = "addr"
	FORMAT_INTERNAL           = "internal"
	FORMAT_CUSTOM             = "custom"
	SRC_OR_DST                = "SRC_OR_DST"
	FORMAT_UINT32_CSV_SPECIAL = "uint32csv"

	NOTIFY_TYPE_EMAIL = "email"
	NOTIFY_TYPE_URL   = "url"
	NOTIFY_TYPE_SLACK = "slack"

	ACT_MIN    = "minute"
	ACT_HOUR   = "hour"
	ACT_SECOND = "second"

	OP_EQUALS                 = "="
	OP_NOT_EQUALS             = "<>"
	OP_LOGICAL_AND            = "&"
	OP_LOGICAL_OR             = "|"
	OP_LESS_THAN              = "<"
	OP_GREATER_THAN           = ">"
	OP_LESS_THAN_OR_EQUALS    = "<="
	OP_GREATER_THAN_OR_EQUALS = ">="
	OP_NOT_CONTAINS           = "NOT ILIKE"
	OP_CONTAINS               = "ILIKE"
	OP_NOT_CONTAINS_RE        = "!~"
	OP_CONTAINS_RE            = "~"
	OP_NOT_CONTAINS_RE_STAR   = "!~*"
	OP_CONTAINS_RE_STAR       = "~*"

	CONS_OR  = "Any"
	CONS_AND = "All"

	ACTIVATE_MODE_TYPE_BASELINE = "baseline"
	ACTIVATE_MODE_TYPE_STATIC   = "static"
	ACTIVATE_MODE_TYPE_EXIST    = "exist"

	METRIC_FIRST  = "metric_0"
	METRIC_SECOND = "metric_1"
	METRIC_THIRD  = "metric_2"

	ACT_NOT_USED_BASELINE                           = 0
	ACT_BASELINE_MISSING_SKIP                       = 1
	ACT_BASELINE_MISSING_TRIGGER                    = 2
	ACT_BASELINE_USED_FOUND                         = 3
	ACT_BASELINE_MISSING_DEFAULT                    = 4
	ACT_BASELINE_MISSING_LOWEST                     = 5
	ACT_BASELINE_MISSING_HIGHEST                    = 6
	ACT_BASELINE_NOT_FOUND_EXISTS                   = 7
	ACT_CURRENT_MISSING_SKIP                        = 8
	ACT_CURRENT_MISSING_TRIGGER                     = 9
	ACT_CURRENT_USED_FOUND                          = 10
	ACT_CURRENT_MISSING_DEFAULT                     = 11
	ACT_CURRENT_MISSING_LOWEST                      = 12
	ACT_CURRENT_MISSING_HIGHEST                     = 13
	ACT_CURRENT_NOT_FOUND_EXISTS                    = 14
	ACT_BASELINE_MISSING_DEFAULT_INSTEAD_OF_LOWEST  = 15
	ACT_BASELINE_MISSING_DEFAULT_INSTEAD_OF_HIGHEST = 16
	ACT_CURRENT_MISSING_DEFAULT_INSTEAD_OF_LOWEST   = 17
	ACT_CURRENT_MISSING_DEFAULT_INSTEAD_OF_HIGHEST  = 18

	ActivateDebugNotSet = 0

	ALARM_STATE_ALARM   = "ALARM"
	ALARM_STATE_CLEAR   = "CLEAR"
	ALARM_STATE_ACK_REQ = "ACK_REQ"
)

var (
	FieldTypes = map[string]string{
		IN_BYTES:               FORMAT_UINT64,
		IN_PKTS:                FORMAT_UINT64,
		OUT_BYTES:              FORMAT_UINT64,
		OUT_PKTS:               FORMAT_UINT64,
		BOTH_PKTS:              FORMAT_UINT64,
		BOTH_BYTES:             FORMAT_UINT64,
		FLOWS:                  FORMAT_UINT64,
		INPUT_INTERFACE_SPEED:  FORMAT_UINT64,
		OUTPUT_INTERFACE_SPEED: FORMAT_UINT64,

		HEADERLEN:              FORMAT_UINT32,
		IPSIZE:                 FORMAT_UINT32,
		IPV4_DST_ADDR:          FORMAT_UINT32,
		IPV4_SRC_ADDR:          FORMAT_UINT32,
		INET_DST_ADDR:          FORMAT_UINT32,
		INET_SRC_ADDR:          FORMAT_UINT32,
		DST_AS:                 FORMAT_UINT32,
		SRC_AS:                 FORMAT_UINT32,
		VLAN_IN:                FORMAT_UINT32,
		VLAN_OUT:               FORMAT_UINT32,
		IPV4_DST_NEXT_HOP:      FORMAT_UINT32,
		IPV4_SRC_NEXT_HOP:      FORMAT_UINT32,
		INET_DST_NEXT_HOP:      FORMAT_UINT32,
		INET_SRC_NEXT_HOP:      FORMAT_UINT32,
		MPLS_TYPE:              FORMAT_UINT32,
		TCP_RETRANSMIT:         FORMAT_UINT32,
		SAMPLE_RATE:            FORMAT_UINT32,
		DEVICE_ID:              FORMAT_UINT32,
		I_DEVICE_ID:            FORMAT_UINT32,
		SRC_NEXTHOP:            FORMAT_UINT32,
		DST_NEXTHOP:            FORMAT_UINT32,
		INET_FAMILY:            FORMAT_UINT32,
		SRC_ROUTE_LENGTH:       FORMAT_UINT32,
		DST_ROUTE_LENGTH:       FORMAT_UINT32,
		PACKETSIZE:             FORMAT_UINT32,
		SRC_CDN:                FORMAT_UINT32,
		DST_CDN:                FORMAT_UINT32,
		SRC_CONNECT_TYPE:       FORMAT_UINT32,
		DST_CONNECT_TYPE:       FORMAT_UINT32,
		SRC_NETWORK_BNDRY:      FORMAT_UINT32,
		DST_NETWORK_BNDRY:      FORMAT_UINT32,
		ULT_EXIT_NETWORK_BNDRY: FORMAT_UINT32,
		ULT_EXIT_CONNECT_TYPE:  FORMAT_UINT32,
		TRAF_TERM:              FORMAT_UINT32,
		TRAF_ORIG:              FORMAT_UINT32,
		HOST_DIR:               FORMAT_UINT32,
		TRAF_PROF:              FORMAT_UINT32,

		L4_DST_PORT:   FORMAT_USHORT,
		L4_SRC_PORT:   FORMAT_USHORT,
		SRC_GEO:       FORMAT_USHORT,
		DST_GEO:       FORMAT_USHORT,
		TCP_FLAGS:     FORMAT_USHORT,
		TCP_FLAGS_RAW: FORMAT_USHORT,

		PROTOCOL:      FORMAT_BYTE,
		TOS:           FORMAT_BYTE,
		PROTOCOL_NAME: FORMAT_BYTE,

		DST_MAC:            FORMAT_STRING,
		DST_ETH_MAC:        FORMAT_STRING,
		SRC_MAC:            FORMAT_STRING,
		SRC_ETH_MAC:        FORMAT_STRING,
		SRC_FLOW_TAGS:      FORMAT_STRING,
		DST_FLOW_TAGS:      FORMAT_STRING,
		DST_BGP_ASPATH:     FORMAT_STRING,
		DST_BGP_COMMUNITY:  FORMAT_STRING,
		SRC_BGP_ASPATH:     FORMAT_STRING,
		SRC_BGP_COMMUNITY:  FORMAT_STRING,
		SRC_GEO_REGION:     FORMAT_STRING,
		DST_GEO_REGION:     FORMAT_STRING,
		SRC_GEO_CITY:       FORMAT_STRING,
		DST_GEO_CITY:       FORMAT_STRING,
		SRC_AS_NAME:        FORMAT_STRING,
		DST_AS_NAME:        FORMAT_STRING,
		SRC_SECOND_AS_NAME: FORMAT_STRING,
		DST_SECOND_AS_NAME: FORMAT_STRING,
		SRC_THIRD_AS_NAME:  FORMAT_STRING,
		DST_THIRD_AS_NAME:  FORMAT_STRING,
		SRC_NEXTHOP_NAME:   FORMAT_STRING,
		DST_NEXTHOP_NAME:   FORMAT_STRING,
		INPUT_SNMP_ALIAS:   FORMAT_STRING,
		INPUT_SNMP_DESC:    FORMAT_STRING,
		OUTPUT_SNMP_ALIAS:  FORMAT_STRING,
		OUTPUT_SNMP_DESC:   FORMAT_STRING,
		OUTPUT_PROVIDER:    FORMAT_STRING,
		INPUT_PROVIDER:     FORMAT_STRING,
		DEVICE_NAME:        FORMAT_STRING,
		I_DEVICE_NAME:      FORMAT_STRING,
		DEVICE_SITE_NAME:   FORMAT_STRING,
		I_DEVICE_SITE_NAME: FORMAT_STRING,
		DEVICE_TYPE:        FORMAT_STRING,
		DEVICE_LABEL:       FORMAT_STRING,

		ANY_COUNTRY:         SRC_OR_DST,
		ANY_AS_NUMBER:       SRC_OR_DST,
		ANY_AS_NAME:         SRC_OR_DST,
		ANY_FLOW_TAG:        SRC_OR_DST,
		ANY_L4_PORT:         SRC_OR_DST,
		ANY_MAC_ADDR:        SRC_OR_DST,
		ANY_IP_ADDR:         SRC_OR_DST,
		ANY_INTERFACE_DESC:  SRC_OR_DST,
		ANY_INTERFACE_ALIAS: SRC_OR_DST,
		ANY_NETWORK_BNDRY:   SRC_OR_DST,
		ANY_ROUTE_PREFIX:    SRC_OR_DST,
		ANY_PROVIDER:        SRC_OR_DST,
		ANY_CDN:             SRC_OR_DST,
		ANY_CONNECT_TYPE:    SRC_OR_DST,
		ANY_TRAF:            SRC_OR_DST,
		ANY_THREAT_HOST:     SRC_OR_DST,
		ANY_THREAT_BNETCC:   SRC_OR_DST,
		AnyVRFName:          SRC_OR_DST,
		AnyVRFRd:            SRC_OR_DST,
		AnyVRFRt:            SRC_OR_DST,
		AnyVRFExtRd:         SRC_OR_DST,

		INPUT_PORT:  FORMAT_UINT32_CSV_SPECIAL,
		OUTPUT_PORT: FORMAT_UINT32_CSV_SPECIAL,
		ANY_PORT:    FORMAT_UINT32_CSV_SPECIAL,
	}

	AnyTypeSwapSrc = map[string]string{
		ANY_COUNTRY:         SRC_GEO,
		ANY_AS_NUMBER:       SRC_AS,
		ANY_AS_NAME:         SRC_AS_NAME,
		ANY_FLOW_TAG:        SRC_FLOW_TAGS,
		ANY_L4_PORT:         L4_SRC_PORT,
		ANY_MAC_ADDR:        SRC_MAC,
		ANY_IP_ADDR:         INET_SRC_ADDR,
		ANY_PORT:            INPUT_PORT,
		ANY_INTERFACE_DESC:  INPUT_SNMP_DESC,
		ANY_INTERFACE_ALIAS: INPUT_SNMP_ALIAS,
		ANY_NETWORK_BNDRY:   SRC_NETWORK_BNDRY,
		ANY_ROUTE_PREFIX:    INET_SRC_ROUTE_PREFIX,
		ANY_PROVIDER:        INPUT_PROVIDER,
		ANY_CONNECT_TYPE:    SRC_CONNECT_TYPE,
		ANY_CDN:             SRC_CDN,
		ANY_TRAF:            TRAF_ORIG,
		ANY_THREAT_HOST:     SRC_THREAT_HOST,
		ANY_THREAT_BNETCC:   SRC_THREAT_BNETCC,
		AnyVRFName:          InputVRFName,
		AnyVRFRd:            InputVRFRd,
		AnyVRFRt:            InputVRFRt,
		AnyVRFExtRd:         InputVRF,
	}

	AnyTypeSwapDst = map[string]string{
		ANY_COUNTRY:         DST_GEO,
		ANY_AS_NUMBER:       DST_AS,
		ANY_AS_NAME:         DST_AS_NAME,
		ANY_FLOW_TAG:        DST_FLOW_TAGS,
		ANY_L4_PORT:         L4_DST_PORT,
		ANY_MAC_ADDR:        DST_MAC,
		ANY_IP_ADDR:         INET_DST_ADDR,
		ANY_PORT:            OUTPUT_PORT,
		ANY_INTERFACE_DESC:  OUTPUT_SNMP_DESC,
		ANY_INTERFACE_ALIAS: OUTPUT_SNMP_ALIAS,
		ANY_NETWORK_BNDRY:   DST_NETWORK_BNDRY,
		ANY_ROUTE_PREFIX:    INET_DST_ROUTE_PREFIX,
		ANY_PROVIDER:        OUTPUT_PROVIDER,
		ANY_CONNECT_TYPE:    DST_CONNECT_TYPE,
		ANY_CDN:             DST_CDN,
		ANY_TRAF:            TRAF_TERM,
		ANY_THREAT_HOST:     DST_THREAT_HOST,
		ANY_THREAT_BNETCC:   DST_THREAT_BNETCC,
		AnyVRFName:          OutputVRFName,
		AnyVRFRd:            OutputVRFRd,
		AnyVRFRt:            OutputVRFRt,
		AnyVRFExtRd:         OutputVRF,
	}

	FlippedFB = map[int]int{
		ACT_NOT_USED_BASELINE:                           ACT_NOT_USED_BASELINE,
		ACT_BASELINE_MISSING_SKIP:                       ACT_CURRENT_MISSING_SKIP,
		ACT_BASELINE_MISSING_TRIGGER:                    ACT_CURRENT_MISSING_TRIGGER,
		ACT_BASELINE_USED_FOUND:                         ACT_CURRENT_USED_FOUND,
		ACT_BASELINE_MISSING_DEFAULT:                    ACT_CURRENT_MISSING_DEFAULT,
		ACT_BASELINE_MISSING_DEFAULT_INSTEAD_OF_LOWEST:  ACT_CURRENT_MISSING_DEFAULT_INSTEAD_OF_LOWEST,
		ACT_BASELINE_MISSING_DEFAULT_INSTEAD_OF_HIGHEST: ACT_CURRENT_MISSING_DEFAULT_INSTEAD_OF_HIGHEST,
		ACT_BASELINE_MISSING_LOWEST:                     ACT_CURRENT_MISSING_LOWEST,
		ACT_BASELINE_MISSING_HIGHEST:                    ACT_CURRENT_MISSING_HIGHEST,
		ACT_BASELINE_NOT_FOUND_EXISTS:                   ACT_CURRENT_NOT_FOUND_EXISTS,
	}

	// all fields are splittable (by ',') unless specified here
	SplitFields = map[string]bool{
		SRC_GEO_REGION:          false,
		DST_GEO_REGION:          false,
		SRC_GEO_CITY:            false,
		DST_GEO_CITY:            false,
		INPUT_SNMP_DESC:         false,
		OUTPUT_SNMP_DESC:        false,
		INPUT_SNMP_ALIAS:        false,
		OUTPUT_SNMP_ALIAS:       false,
		OUTPUT_PROVIDER:         false,
		INPUT_PROVIDER:          false,
		I_DEVICE_SITE_NAME:      false,
		SRC_AS_NAME:             false,
		DST_AS_NAME:             false,
		SRC_NEXTHOP_NAME:        false,
		DST_NEXTHOP_NAME:        false,
		SRC_SECOND_AS_NAME:      false,
		DST_SECOND_AS_NAME:      false,
		SRC_THIRD_AS_NAME:       false,
		DST_THIRD_AS_NAME:       false,
		ULT_EXIT_SITE:           false,
		ULT_EXIT_INTERFACE_DESC: false,
		ULT_EXIT_SNMP_ALIAS:     false,
		ULT_EXIT_PROVIDER:       false,
		IN_BYTES:                false,
		OUT_BYTES:               false,
		IN_PKTS:                 false,
		OUT_PKTS:                false,
		TRAF_PROF:               false,
		FIREEYE_SRC:             false,
		FIREEYE_DST:             false,

		// https://github.com/kentik/chf-alert/issues/118
		INPUT_PORT:  false,
		OUTPUT_PORT: false,
		ANY_PORT:    false,

		// ipset
		IPV4_SRC_ADDR: false,
		IPV4_DST_ADDR: false,
		INET_SRC_ADDR: false,
		INET_DST_ADDR: false,
		ANY_IP_ADDR:   false,

		IPV4_DST_NEXT_HOP: false,
		IPV4_SRC_NEXT_HOP: false,
		INET_DST_NEXT_HOP: false,
		INET_SRC_NEXT_HOP: false,

		INET_DST_ROUTE_PREFIX: false,
		INET_SRC_ROUTE_PREFIX: false,
		ANY_ROUTE_PREFIX:      false,
		IPV4_DST_ROUTE_PREFIX: false,
		IPV4_SRC_ROUTE_PREFIX: false,
	}

	// Corresponding to the metrics that can be encoded with CIDR prefixes in
	// chnode_v2/dictionary/cidrMetrics.js
	DimensionsWithEncodedCidrPrefix = []string{
		DIMENSION_IP_SRC_CIDR,
		DIMENSION_IP_DST_CIDR,
		DIMENSION_TOPFLOWSIP_CIDR,
		DIMENSION_SRC_NEXTHOP_IP_CIDR,
		DIMENSION_DST_NEXTHOP_IP_CIDR,
	}

	AltMeasureNames = map[string]string{
		MEASURE_CUSTOM_CLIENT_LATENCY:       "CLIENT_NW_LATENCY_MS",
		MEASURE_CUSTOM_SERVER_LATENCY:       "SERVER_NW_LATENCY_MS",
		MEASURE_CUSTOM_APP_LATENCY:          "APPL_LATENCY_MS",
		MEASURE_CUSTOM_FRAGMENTS:            "FRAGMENTS",
		MEASURE_CUSTOM_RETRANSMITS:          "RETRANSMITTED_OUT_PKTS",
		MEASURE_CUSTOM_OOO:                  "OOORDER_IN_PKTS",
		MEASURE_CUSTOM_REPEATED_RETRANSMITS: "REPEATED_RETRANSMITS",
		MEASURE_CUSTOM_RETRANSMIT_PERC:      "RETRANSMITTED_OUT_PKTS",
		MEASURE_CUSTOM_OOO_PERC:             "OOORDER_IN_PKTS",
		MEASURE_CUSTOM_FRAGMENT_PERC:        "FRAGMENTS",
		MEASURE_CUSTOM_FPEX_LATENCY_MS:      "FPEX_LATENCY_MS",
	}
)

func SeverityLevelValid(level string) bool {
	for _, sv := range SeverityLevels {
		if sv == level {
			return true
		}
	}
	return false
}

func SeverityLevelAGreaterThanB(severityA, severityB string) bool {
	return SeverityLevelToInt[severityA] > SeverityLevelToInt[severityB]
}

func IsDimensionWithEncodedCidrPrefix(dim string) (prefix string, ok bool) {
	for _, d := range DimensionsWithEncodedCidrPrefix {
		if strings.HasPrefix(dim, d) {
			return d, true
		}
	}
	return "", false
}
