package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type StateFetcher func() (cursor *sink.Cursor, backprocessCompleted bool, headBlockReached bool)

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

func (s *StateStore) Delete() error {
	zlog.Info("deleting output path file", zap.String("output_path", s.outputPath))
	return os.Remove(s.outputPath)
}

func (s *StateStore) Read() (cursor string, block bstream.BlockRef, err error) {
	content, err := os.ReadFile(s.outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", bstream.BlockRefEmpty, nil
		}

		return "", nil, fmt.Errorf("read file: %w", err)
	}

	if err := yaml.Unmarshal(content, s.state); err != nil {
		return "", nil, fmt.Errorf("unmarshal state file %q: %w", s.outputPath, err)
	}

	// Make all values loaded in local time until all state file migrates to using local time
	s.state.StartedAt = s.state.StartedAt.Local()
	s.state.RestartedAt = s.state.RestartedAt.Local()
	s.state.LastSyncedAt = s.state.LastSyncedAt.Local()
	s.state.BackprocessingCompletedAt = s.state.BackprocessingCompletedAt.Local()
	s.state.HeadBlockReachedAt = s.state.HeadBlockReachedAt.Local()
	return s.state.Cursor, bstream.NewBlockRef(s.state.Block.ID, s.state.Block.Number), nil
}

func (s *StateStore) Start(each time.Duration) {
	zlog.Info("starting state persistent storage service", zap.Duration("runs_each", each))

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

		restartedAt := time.Now().Local()

		if s.state.StartedAt.IsZero() {
			s.state.StartedAt = restartedAt
		}
		s.state.RestartedAt = restartedAt

		for {
			select {
			case <-ticker.C:
				if err := s.SyncNow(); err != nil {
					s.Shutdown(err)
				}

			case <-s.Terminating():
				return
			}
		}
	}()
}

func (s *StateStore) SyncNow() error {
	zlog.Debug("saving cursor to output path", zap.String("output_path", s.outputPath))
	cursor, backprocessCompleted, headBlockReached := s.fetcher()

	s.state.Cursor = cursor.String()
	s.state.Block.ID = cursor.Block().ID()
	s.state.Block.Number = cursor.Block().Num()
	s.state.LastSyncedAt = time.Now().Local()

	if backprocessCompleted && s.state.BackprocessingCompletedAt.IsZero() {
		s.state.BackprocessingCompletedAt = s.state.LastSyncedAt
		s.state.BackprocessingDuration = s.state.BackprocessingCompletedAt.Sub(s.state.StartedAt)
	}

	if headBlockReached && s.state.HeadBlockReachedAt.IsZero() {
		s.state.HeadBlockReachedAt = s.state.LastSyncedAt
		s.state.HeadBlockReachedDuration = s.state.HeadBlockReachedAt.Sub(s.state.StartedAt)
	}

	content, err := yaml.Marshal(s.state)
	if err != nil {
		return fmt.Errorf("unable to marshal state: %w", err)
	}

	if err := os.WriteFile(s.outputPath, content, os.ModePerm); err != nil {
		return fmt.Errorf("unable to write state file: %w", err)
	}

	return nil
}

type syncState struct {
	Cursor string     `yaml:"cursor"`
	Block  blockState `yaml:"block"`
	// StartedAt is the time this process was launching initially without accounting to any restart, once set, this
	// value, it's never re-written (unless the file does not exist anymore).
	StartedAt time.Time `yaml:"started_at,omitempty"`
	// RestartedAt is the time this process was last launched meaning it's reset each time the process start. This value
	// in contrast to `StartedAt` change over time each time the process is restarted.
	RestartedAt               time.Time     `yaml:"restarted_at,omitempty"`
	LastSyncedAt              time.Time     `yaml:"last_synced_at,omitempty"`
	BackprocessingCompletedAt time.Time     `yaml:"backprocessing_completed_at,omitempty"`
	BackprocessingDuration    time.Duration `yaml:"backprocessing_duration,omitempty"`
	HeadBlockReachedAt        time.Time     `yaml:"head_block_reached_at,omitempty"`
	HeadBlockReachedDuration  time.Duration `yaml:"head_block_reached_duration,omitempty"`
}

type blockState struct {
	ID     string `yaml:"id"`
	Number uint64 `yaml:"number"`
}

func (s *StateStore) Close() {
	// There is a slight chance of race with the loop ticker, but let's hope it will never break the file
	// FIXME: Maybe dangerous?
	s.Shutdown(s.SyncNow())
}
