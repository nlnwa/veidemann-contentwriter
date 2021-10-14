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

package settings

import (
	"github.com/nlnwa/gowarc"
	"github.com/spf13/viper"
)

type Settings interface {
	HostName() string
	WarcDir() string
	WarcWriterPoolSize() int
	WorkDir() string
	TerminationGracePeriodSeconds() int
	WarcVersion() *gowarc.WarcVersion
	FlushRecord() bool
	UseStrictValidation() bool
}

type ViperSettings struct {
}

func (s ViperSettings) HostName() string {
	return viper.GetString("host-name")
}

func (s ViperSettings) WarcDir() string {
	return viper.GetString("warc-dir")
}

func (s ViperSettings) WarcWriterPoolSize() int {
	return viper.GetInt("warc-writer-pool-size")
}

func (s ViperSettings) WorkDir() string {
	return viper.GetString("work-dir")
}

func (s ViperSettings) TerminationGracePeriodSeconds() int {
	return viper.GetInt("termination-grace-period-seconds")
}

func (s ViperSettings) WarcVersion() *gowarc.WarcVersion {
	v := viper.GetString("warc-version")
	switch v {
	case "1.0":
		return gowarc.V1_0
	case "1.1":
		return gowarc.V1_1
	default:
		panic("Unsupported WARC version: " + v)
	}
}

func (s ViperSettings) FlushRecord() bool {
	return viper.GetBool("flush-record")
}

func (s ViperSettings) UseStrictValidation() bool {
	return viper.GetBool("strict-validation")
}
