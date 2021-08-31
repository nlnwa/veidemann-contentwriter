/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// PatternNameGenerator implements the WarcFileNameGenerator.
type PatternNameGenerator struct {
	Directory string // Directory to store warcfiles. Defaults to the empty string
	Prefix    string // Prefix available to be used in pattern. Defaults to the empty string
	Serial    int32  // Serial number available for use in pattern. It is atomically increased with every generated file name.
	//Pattern   string // Pattern for generated file name. Defaults to: "%{prefix}s%{ts}s-%04{serial}d-%{ip}s.warc"
}

func (g *PatternNameGenerator) NewWarcfileName() string {
	prefix := g.Prefix
	if g.Directory != "" {
		prefix = g.Directory + "/" + prefix
	}
	name := fmt.Sprintf("%s%s-%04d-%s.warc", prefix, UTCNow14(), atomic.AddInt32(&g.Serial, 1), GetOutboundIP())
	return name
}

func UTCNow14() string {
	return time.Now().In(time.UTC).Format("20060102150405")
}

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
