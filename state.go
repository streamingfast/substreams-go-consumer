package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type StateFetcher func() (cursor string, block bstream.BlockRef, backprocessCompleted bool)

type StateStore struct {
	*shutter.Shutter
	fetcher    StateFetcher
	outputPath string

	state *syncState
}

func NewStateStore(outputPath string, fetcher StateFetcher) *StateStore {
	return &StateStore{
		Shutter:    shutter.New(),
		fetcher:    fetcher,
		outputPath: outputPath,

		state: &syncState{
			Cursor: "",
			Block: blockState{
				ID:     "",
				Number: 0,
			},
		},
	}
}

func (s *StateStore) Read() (cursor string, block bstream.BlockRef, err error) {
	content, err := os.ReadFile(s.outputPath)
	if os.IsNotExist(err) {
		return "", bstream.BlockRefEmpty, nil
	}

	if err := yaml.Unmarshal(content, s.state); err != nil {
		return "", nil, fmt.Errorf("unmarshal state file %q: %w", s.outputPath, err)
	}

	return s.state.Cursor, bstream.NewBlockRef(s.state.Block.ID, s.state.Block.Number), nil
}

func (s *StateStore) Start(each time.Duration) {
	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	if err := os.MkdirAll(filepath.Dir(s.outputPath), os.ModePerm); err != nil {
		s.Shutdown(fmt.Errorf("unable to create directories for output path: %w", err))
		return
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		s.state.StartedAt = time.Now()

		for {
			select {
			case <-ticker.C:
				zlog.Debug("saving cursor to output path", zap.String("output_path", s.outputPath))
				cursor, block, backprocessCompleted := s.fetcher()

				s.state.Cursor = cursor
				s.state.Block.ID = block.ID()
				s.state.Block.Number = block.Num()
				s.state.LastSyncedAt = time.Now()

				if backprocessCompleted && s.state.BackprocessingCompletedAt.IsZero() {
					s.state.BackprocessingCompletedAt = s.state.LastSyncedAt
					s.state.BackprocessingDuration = s.state.BackprocessingCompletedAt.Sub(s.state.StartedAt)
				}

				content, err := yaml.Marshal(s.state)
				if err != nil {
					s.Shutdown(fmt.Errorf("unable to marshal state: %w", err))
					return
				}

				if err := os.WriteFile(s.outputPath, content, os.ModePerm); err != nil {
					s.Shutdown(fmt.Errorf("unable to write state file: %w", err))
					return
				}

			case <-s.Terminating():
				break
			}
		}
	}()
}

type syncState struct {
	Cursor                    string        `yaml:"cursor"`
	Block                     blockState    `yaml:"block"`
	StartedAt                 time.Time     `yaml:"started_at,omitempty"`
	LastSyncedAt              time.Time     `yaml:"last_synced_at,omitempty"`
	BackprocessingCompletedAt time.Time     `yaml:"backprocessing_completed_at,omitempty"`
	BackprocessingDuration    time.Duration `ymal:"backprocessing_duration,omitempty"`
}

type blockState struct {
	ID     string `yaml:"id"`
	Number uint64 `yaml:"number"`
}

func (s *StateStore) Close() {
	s.Shutdown(nil)
}
