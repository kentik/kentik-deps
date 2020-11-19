package patricia

// TODO: fix outdated tests?
import (
	"bytes"
	"github.com/kentik/chf/pkg/util/fetch"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/kentik/golog/logger"
	"github.com/kentik/patricia"
	"github.com/stretchr/testify/assert"
)

const (
	GEO_FILE    = "patricia_test.csv"
	ASN_FILE    = "asn.csv"
	ASN6_FILE   = "asn6.csv"
	FORCE_USE   = false
	IPV4ADDRLEN = 32
	IPV6ADDRLEN = 128
	// TAG_URL             = "http://127.0.0.1:8080/api/v3/company/1433/tags"
	// TAG_AUTH            = "1433:superx2:1641"
	// TAG_SAMPLE_RATE     = 10
)

type ExpectedGeoData struct {
	IP      string
	Country uint32
	Region  uint32
	City    uint32
}

func TestPrefix(t *testing.T) {
	tests := []string{
		`10.10.10.10\n`,
		`10.10.10.10x`,
		``,
		`woofoo`,
		`woofoo/10`,
		`woofoo/woo`,
		`1011.1011.1011.1011/24`,
		`.../24`,
		`/`,
	}

	for _, s := range tests {
		v4, v6, err := patricia.ParseIPFromString(s)
		assert.Error(t, err, s)
		assert.Nil(t, v4)
		assert.Nil(t, v6)
	}
}

func TestLoadGeoCSV(t *testing.T) {
	logger.SetLogName("[CHF_TEST]")
	log := logger.New(logger.CfgLevels["debug"])

	geo, err := OpenGeo(GEO_FILE, FORCE_USE, log)
	if err != nil {
		t.Fatal(err)
	}
	defer geo.Close()
	defer os.Remove(GEO_FILE + fetch.MMAP_SUFFIX)
	defer os.Remove(GEO_FILE + ".lock")

	tests := []ExpectedGeoData{
		{"0.0.0.0", 11520, 1, 1},
		{"1.0.5.1", 16725, 34, 271},
		{"2c0f:ffc9::1", 19797, 2963, 120766},
		{"2c0f:fff0::1", 20039, 1004, 14872},
	}

	for _, d := range tests {
		ip := net.ParseIP(d.IP)
		node, err := geo.SearchBestFromHostGeo(ip)
		if err != nil {
			t.Fatal(err)
		}

		if country := GetCountry(node); country != d.Country {
			fmt.Printf("IP %s: expected country %d, got %d\n", d.IP, d.Country, country)
		}

		if region := GetRegion(node); region != d.Region {
			fmt.Printf("IP %s: expected region %d, got %d\n", d.IP, d.Region, region)
		}

		if city := GetCity(node); city != d.City {
			fmt.Printf("IP %s: expected region %d, got %d\n", d.IP, d.City, city)
		}
	}
}

func TestOpenCloseASN(t *testing.T) {
	asn, err := OpenASN(ASN_FILE, ASN6_FILE, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer asn.Close()
}

// @TOOD -- make work.
// Needs spanning geo file to work I think.
func TestGeo(t *testing.T) {
	search := []string{"1.0.199.1"}
	searchRange := []string{"1.0.199.0/0"}
	searchNum := []uint32{19282, 21843, 21843, 21843, 21843, 21843}
	searchNumCity := []uint32{117, 7201, 4294, 7436, 4294, 4304}
	searchNumRegion := []uint32{61, 460, 213, 495, 213, 213}

	var content bytes.Buffer
	for i, ip := range searchRange {
		content.WriteString(fmt.Sprintf("%s,%d,%d,%d\n", ip, searchNum[i], searchNumCity[i], searchNumRegion[i]))
	}

	tmpfile, err := ioutil.TempFile("", "testFile")
	assert.NoError(t, err)

	defer os.Remove(tmpfile.Name()) // clean up

	tmpfile.Write(content.Bytes())
	assert.NoError(t, err)

	err = tmpfile.Close()
	assert.NoError(t, err)

	geo, err := OpenGeo(tmpfile.Name(), true, nil)
	assert.NoError(t, err)

	defer geo.Close()

	for i, s := range search {
		v4Addr, v6Addr, err := patricia.ParseIPFromString(s)
		assert.NoError(t, err)
		var node *NodeGeo
		if v4Addr != nil {
			node, err = geo.SearchBestFromHostGeo(net.IPv4(byte(v4Addr.Address>>24), byte(v4Addr.Address>>16), byte(v4Addr.Address>>8), byte(v4Addr.Address)))
			assert.Error(t, err, s)
		} else if v6Addr != nil {
			node, err = geo.SearchBestFromHostGeo(net.ParseIP(s))
			assert.Error(t, err, s)
		}

		assert.Nil(t, node)
		if node != nil {
			cntry := GetCountry(node)
			region := GetRegion(node)
			city := GetCity(node)

			if cntry != searchNum[i] {
				t.Errorf("For %s, Country Got %d, Expecting %d", s, cntry, searchNum[i])
			}

			if region != searchNumRegion[i] {
				t.Errorf("For %s, Region Got %d, Expecting %d", s, region, searchNumRegion[i])
			}

			if city != searchNumCity[i] {
				t.Errorf("For %s, City Got %d, Expecting %d", s, city, searchNumCity[i])
			}
		}
	}
}

func TestASNSearch(t *testing.T) {
	asn, err := OpenASN(ASN_FILE, ASN6_FILE, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer asn.Close()

	search := []string{"98.139.183.24", "198.186.190.179", "151.101.124.84", "2606:f280:a100::1", "2603:4000::1"}
	target := []int{26101, 6450, 54113, 395373, 3}

	for i, s := range search {
		v4Addr, v6Addr, err := patricia.ParseIPFromString(s)
		assert.NoError(t, err)
		ip4 := uint32(0)
		var ip6 []byte = nil
		if v4Addr != nil {
			ip4 = v4Addr.Address
		} else if v6Addr != nil {
			ip6 = net.ParseIP(s)
		}

		resultsFound, asn, err := asn.FindBestMatch(ip4, ip6)
		assert.True(t, resultsFound)
		assert.NotNil(t, asn, "Looking for %v -- %s", target[i], s)
		if int(asn) != target[i] {
			t.Errorf("For %s, Got %d, Expecting %d", s, asn, target[i])
		}
	}
}

//func BenchmarkGeo(b *testing.B) {

//    logger.SetLogName("[CHF_TEST]")
//    log := logger.New(logger.CfgLevels["debug"])

//    b.StopTimer()
//    geo, err := patricia.OpenGeo(GEO_FILE_OLD, DOWNLOAD_URL, DOWNLOAD_URL_BACKUP, FORCE_USE, log)
//    if err != nil {
//        b.Fatal(err)
//    }
//    defer geo.Close()
//    b.StartTimer()

//    search := []string{"205.193.117.158"}
//    for n := 0; n < b.N; n++ {
//        for _, s := range search {
//            sn := topk.PackIPV4([]byte(s))
//            _, err := geo.SearchBestFromIPv4HostPackedGeo(sn)
//            if err != nil {
//                b.Fatal(err)
//            }
//        }
//    }
//}

func makeIP(a, b, c, d uint32) uint32 {
	return a<<24 + b<<16 + c<<8 + d
}

func mustCIDR(cidr string) *net.IPNet {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return ipnet
}
