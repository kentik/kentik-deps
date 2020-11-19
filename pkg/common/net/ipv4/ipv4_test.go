package ipv4

import "testing"

var str = "1.2.3.4"
var num = uint32(16909060)

func TestPackIPv4(t *testing.T) {
	if PackIPv4(&str) != num {
		t.Fatal("PackIPv4 failed")
	}
}

func TestUnpackIPv4(t *testing.T) {
	if UnpackIPv4(num) != str {
		t.Fatal("UnpackIPv4 failed")
	}
}
