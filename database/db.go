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

package database

import (
	"context"
	"fmt"
	configV1 "github.com/nlnwa/veidemann-api/go/config/v1"
	"github.com/rs/zerolog/log"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
	"time"
)

var logger = log.With().Str("component", "rethinkdb").Logger()

// RethinkDbConnection holds the database connection
type RethinkDbConnection struct {
	connectOpts        r.ConnectOpts
	session            r.QueryExecutor
	maxRetries         int
	waitTimeout        time.Duration
	queryTimeout       time.Duration
	maxOpenConnections int
	batchSize          int
}

type Options struct {
	Username           string
	Password           string
	Database           string
	UseOpenTracing     bool
	Address            string
	QueryTimeout       time.Duration
	MaxRetries         int
	MaxOpenConnections int
}

// NewRethinkDbConnection creates a new RethinkDbConnection object
func NewRethinkDbConnection(opts Options) *RethinkDbConnection {
	return &RethinkDbConnection{
		connectOpts: r.ConnectOpts{
			Address:        opts.Address,
			Username:       opts.Username,
			Password:       opts.Password,
			Database:       opts.Database,
			InitialCap:     2,
			MaxOpen:        opts.MaxOpenConnections,
			UseOpentracing: opts.UseOpenTracing,
			NumRetries:     10,
			Timeout:        10 * time.Second,
		},
		maxRetries:   opts.MaxRetries,
		waitTimeout:  60 * time.Second,
		queryTimeout: opts.QueryTimeout,
		batchSize:    200,
	}
}

// Connect establishes connections
func (c *RethinkDbConnection) Connect() error {
	var err error
	// Set up database RethinkDbConnection
	c.session, err = r.Connect(c.connectOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to RethinkDB at %s: %w", c.connectOpts.Address, err)
	}
	logger.Info().Msgf("Connected to RethinkDB at %s", c.connectOpts.Address)
	return nil
}

// Close closes the RethinkDbConnection
func (c *RethinkDbConnection) Close() error {
	logger.Info().Msgf("Closing connection to RethinkDB")
	return c.session.(*r.Session).Close()
}

// GetConfigObject fetches a config.ConfigObject referenced by a config.ConfigRef
func (c *RethinkDbConnection) GetConfigObject(ctx context.Context, ref *configV1.ConfigRef) (*configV1.ConfigObject, error) {
	term := r.Table("config").Get(ref.Id)
	res, err := c.execRead(ctx, "get-config-object", &term)
	if err != nil {
		return nil, err
	}
	var result configV1.ConfigObject
	err = res.One(&result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

//// GetConfigsForSelector fetches a list of config.ConfigObject's matching config.Kind and config.Label
//func (c *RethinkDbConnection) GetConfigsForSelector(ctx context.Context, kind configV1.Kind, label *configV1.Label) ([]*configV1.ConfigObject, error) {
//	term := r.Table("config").GetAllByIndex("label", r.Expr([]string{label.Key, label.Value})).
//		Filter(func(row r.Term) r.Term {
//			return row.Field("kind").Eq(kind.String())
//		})
//	res, err := c.execRead(ctx, "get-configs-by-label", &term)
//	if err != nil {
//		return nil, err
//	}
//	defer func() {
//		_ = res.Close()
//	}()
//
//	var configObject configV1.ConfigObject
//	var configObjects []*configV1.ConfigObject
//	for res.Next(&configObject) {
//		//noinspection GoVetCopyLock
//		aCopy := configObject
//		configObjects = append(configObjects, &aCopy)
//	}
//	if err := res.Err(); err != nil {
//		return nil, err
//	}
//
//	return configObjects, nil
//}

// execRead executes the given read term with a timeout
func (c *RethinkDbConnection) execRead(ctx context.Context, name string, term *r.Term) (*r.Cursor, error) {
	q := func(ctx context.Context) (*r.Cursor, error) {
		runOpts := r.RunOpts{
			Context: ctx,
		}
		return term.Run(c.session, runOpts)
	}
	return c.execWithRetry(ctx, name, q)
}

// execWrite executes the given write term with a timeout
func (c *RethinkDbConnection) execWrite(ctx context.Context, name string, term *r.Term) error {
	q := func(ctx context.Context) (*r.Cursor, error) {
		runOpts := r.RunOpts{
			Context:    ctx,
			Durability: "soft",
		}
		_, err := (*term).RunWrite(c.session, runOpts)
		return nil, err
	}
	_, err := c.execWithRetry(ctx, name, q)
	return err
}

// execWithRetry executes given query function repeatedly until successful or max retry limit is reached
func (c *RethinkDbConnection) execWithRetry(ctx context.Context, name string, q func(ctx context.Context) (*r.Cursor, error)) (cursor *r.Cursor, err error) {
	attempts := 0
	logger := logger.With().Str("operation", name).Logger()
out:
	for {
		attempts++
		cursor, err = c.exec(ctx, q)
		if err == nil {
			return
		}
		logger.Warn().Err(err).Int("retries", attempts-1).Msg("")
		switch err {
		case r.ErrQueryTimeout:
			err := c.wait()
			if err != nil {
				logger.Warn().Err(err).Msg("")
			}
		case r.ErrConnectionClosed:
			err := c.Connect()
			if err != nil {
				logger.Warn().Err(err).Msg("")
			}
		default:
			break out
		}
		if attempts > c.maxRetries {
			break
		}
	}
	return nil, fmt.Errorf("failed to %s after %d of %d attempts: %w", name, attempts, c.maxRetries+1, err)
}

// exec executes the given query with a timeout
func (c *RethinkDbConnection) exec(ctx context.Context, q func(ctx context.Context) (*r.Cursor, error)) (*r.Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()
	return q(ctx)
}

// wait waits for database to be fully up date and ready for read/write
func (c *RethinkDbConnection) wait() error {
	waitOpts := r.WaitOpts{
		Timeout: c.waitTimeout,
	}
	_, err := r.DB(c.connectOpts.Database).Wait(waitOpts).Run(c.session)
	return err
}
