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

type Mock struct {
	hostName                      string
	warcDir                       string
	warcWriterPoolSize            int
	workDir                       string
	terminationGracePeriodSeconds int
}

func NewMock(warcDir string, warcWriterPoolSize int) *Mock {
	return &Mock{warcDir: warcDir, warcWriterPoolSize: warcWriterPoolSize}
}

func (m Mock) HostName() string {
	return m.hostName
}

func (m Mock) WarcDir() string {
	return m.warcDir
}

func (m Mock) WarcWriterPoolSize() int {
	return m.warcWriterPoolSize
}

func (m Mock) WorkDir() string {
	return m.workDir
}

func (m Mock) TerminationGracePeriodSeconds() int {
	return m.terminationGracePeriodSeconds
}
