/*
Command nf-dump-pcap decodes NetFlow packets from one or more PCAP files.

Usage:
		nf-dump-pcap [<file>[ .. <file>]]

No flags are available.
*/
package main

import (
	"bytes"
	"flag"
	"log"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
	"github.com/kentik/kentik-deps/pkt/netflow"
	"github.com/kentik/kentik-deps/pkt/netflow/ipfix"
	"github.com/kentik/kentik-deps/pkt/netflow/netflow1"
	"github.com/kentik/kentik-deps/pkt/netflow/netflow5"
	"github.com/kentik/kentik-deps/pkt/netflow/netflow6"
	"github.com/kentik/kentik-deps/pkt/netflow/netflow7"
	"github.com/kentik/kentik-deps/pkt/netflow/netflow9"
	"github.com/kentik/kentik-deps/pkt/netflow/session"
)

func main() {
	flag.Parse()

	for _, arg := range flag.Args() {
		log.Println("reading", arg)

		var r *pcap.Handle
		var err error
		if r, err = pcap.OpenOffline(arg); err != nil {
			log.Printf("error reading %s: %v\n", arg, err)
			continue
		}

		s := session.New()
		d := netflow.NewDecoder(s)

		packetSource := gopacket.NewPacketSource(r, r.LinkType())
		for packet := range packetSource.Packets() {
			log.Println("packet:", packet)

			m, err := d.Read(bytes.NewBuffer(packet.TransportLayer().LayerPayload()))
			if err != nil {
				log.Println("decoder error:", err)
				continue
			}

			switch p := m.(type) {
			case *netflow1.Packet:
				netflow1.Dump(p)

			case *netflow5.Packet:
				netflow5.Dump(p)

			case *netflow6.Packet:
				netflow6.Dump(p)

			case *netflow7.Packet:
				netflow7.Dump(p)

			case *netflow9.Packet:
				netflow9.Dump(p)

			case *ipfix.Message:
				ipfix.Dump(p)
			}
		}
	}
}
