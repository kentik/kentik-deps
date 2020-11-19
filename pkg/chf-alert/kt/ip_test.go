package kt

import "testing"

func TestEnsureAtLeastPrefix(t *testing.T) {
	type args struct {
		ipOrCidr  IPCidr
		minPrefix int
	}
	tests := []struct {
		name    string
		args    args
		want    IPCidr
		wantErr bool
	}{
		{"no prefix, min 32", args{ipOrCidr: "10.99.99.99", minPrefix: 32}, "10.99.99.99/32", false},
		{"no prefix, min 24", args{ipOrCidr: "10.99.99.99", minPrefix: 24}, "10.99.99.0/24", false},
		{"ipcidr/32, min /32", args{ipOrCidr: "10.99.99.99/32", minPrefix: 32}, "10.99.99.99/32", false},
		{"ipcidr/32, min /24", args{ipOrCidr: "10.99.99.99/32", minPrefix: 24}, "10.99.99.0/24", false},
		{"ipcidr/20, min /24", args{ipOrCidr: "10.99.99.99/20", minPrefix: 24}, "10.99.96.0/20", false},
		{"bad ipcidr", args{ipOrCidr: "10.99.99.99/0"}, "", true},
		{"bad ipcidr", args{ipOrCidr: "10.99.99.99/"}, "", true},
		{"bad ipcidr", args{ipOrCidr: "10.99.99.99/nope"}, "", true},
		{"bad ipcidr", args{ipOrCidr: "10./99.99.99"}, "", true},
		{"bad ipcidr", args{ipOrCidr: "10.99.99.99/"}, "", true},
		{"bad ipcidr", args{ipOrCidr: "/24"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EnsureAtLeastPrefix(tt.args.ipOrCidr, tt.args.minPrefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("EnsureAtLeastPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("EnsureAtLeastPrefix() got = %v, want %v", got, tt.want)
			}
		})
	}
}
