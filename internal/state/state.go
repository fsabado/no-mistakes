// Package state provides a JSON-file-backed run state store for the
// dagu-driven pipeline path. It replaces SQLite for on-demand runs.
package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Round mirrors db.StepRound for JSON serialisation.
type Round struct {
	ID                 string  `json:"id"`
	Round              int     `json:"round"`
	Trigger            string  `json:"trigger"`
	FindingsJSON       string  `json:"findings_json,omitempty"`
	UserFindingsJSON   string  `json:"user_findings_json,omitempty"`
	SelectedFindingIDs string  `json:"selected_finding_ids,omitempty"`
	SelectionSource    string  `json:"selection_source,omitempty"`
	FixSummary         *string `json:"fix_summary,omitempty"`
	DurationMS         int64   `json:"duration_ms,omitempty"`
	CreatedAt          int64   `json:"created_at"`
}

// StepState holds per-step results for a run.
type StepState struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Status string  `json:"status,omitempty"`
	Rounds []Round `json:"rounds,omitempty"`
}

// RunIntent mirrors db.RunIntent.
type RunIntent struct {
	Summary   string  `json:"summary"`
	Source    string  `json:"source"`
	SessionID string  `json:"session_id,omitempty"`
	Score     float64 `json:"score"`
}

// RunState is the top-level state file written by prepare and mutated by steps.
type RunState struct {
	RunID         string               `json:"run_id"`
	RepoDir       string               `json:"repo_dir"`
	WorktreeDir   string               `json:"worktree_dir"`
	Branch        string               `json:"branch"`
	BaseSHA       string               `json:"base_sha"`
	HeadSHA       string               `json:"head_sha"`
	DefaultBranch string               `json:"default_branch"`
	UpstreamURL   string               `json:"upstream_url"`
	Intent        *string              `json:"intent,omitempty"`
	IntentSource  *string              `json:"intent_source,omitempty"`
	IntentScore   *float64             `json:"intent_score,omitempty"`
	PRURL         *string              `json:"pr_url,omitempty"`
	Steps         map[string]StepState `json:"steps,omitempty"`
}

// Store is the interface steps use to read/write run state.
// It mirrors the DB methods actually called by pipeline steps.
type Store interface {
	GetRun(id string) (*RunState, error)
	UpdateRunHeadSHA(runID, sha string) error
	UpdateRunIntent(runID string, intent RunIntent) error
	UpdateRunPRURL(runID, url string) error
	GetStepsByRun(runID string) ([]StepState, error)
	GetRoundsByStep(stepID string) ([]Round, error)
	AppendRound(stepID string, round Round) error
	EnsureStep(runID, stepName string) (string, error)
}

// FileStore implements Store over a JSON file.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// New returns a FileStore for the given state file path.
func New(path string) *FileStore {
	return &FileStore{path: path}
}

// DefaultPath returns $NM_RUN_DIR/state.json or a temp-dir fallback.
func DefaultPath(runID string) string {
	if dir := os.Getenv("NM_RUN_DIR"); dir != "" {
		return filepath.Join(dir, "state.json")
	}
	return filepath.Join(os.TempDir(), "nm-"+runID, "state.json")
}

func (s *FileStore) load() (*RunState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	var rs RunState
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	return &rs, nil
}

func (s *FileStore) save(rs *RunState) error {
	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("mkdir state dir: %w", err)
	}
	return os.WriteFile(s.path, data, 0o644)
}

func (s *FileStore) GetRun(id string) (*RunState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return nil, err
	}
	if rs.RunID != id {
		return nil, fmt.Errorf("run ID mismatch: want %s got %s", id, rs.RunID)
	}
	return rs, nil
}

func (s *FileStore) UpdateRunHeadSHA(runID, sha string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return err
	}
	rs.HeadSHA = sha
	return s.save(rs)
}

func (s *FileStore) UpdateRunIntent(runID string, intent RunIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return err
	}
	rs.Intent = &intent.Summary
	rs.IntentSource = &intent.Source
	rs.IntentScore = &intent.Score
	return s.save(rs)
}

func (s *FileStore) UpdateRunPRURL(runID, url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return err
	}
	rs.PRURL = &url
	return s.save(rs)
}

func (s *FileStore) GetStepsByRun(runID string) ([]StepState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return nil, err
	}
	out := make([]StepState, 0, len(rs.Steps))
	for _, ss := range rs.Steps {
		out = append(out, ss)
	}
	return out, nil
}

func (s *FileStore) GetRoundsByStep(stepID string) ([]Round, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return nil, err
	}
	for _, ss := range rs.Steps {
		if ss.ID == stepID {
			return ss.Rounds, nil
		}
	}
	return nil, nil
}

func (s *FileStore) AppendRound(stepID string, round Round) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return err
	}
	if rs.Steps == nil {
		rs.Steps = make(map[string]StepState)
	}
	for name, ss := range rs.Steps {
		if ss.ID == stepID {
			round.ID = fmt.Sprintf("%s-r%d", stepID, len(ss.Rounds)+1)
			round.CreatedAt = time.Now().UnixMilli()
			ss.Rounds = append(ss.Rounds, round)
			rs.Steps[name] = ss
			return s.save(rs)
		}
	}
	return fmt.Errorf("step ID %s not found", stepID)
}

func (s *FileStore) EnsureStep(runID, stepName string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, err := s.load()
	if err != nil {
		return "", err
	}
	if rs.Steps == nil {
		rs.Steps = make(map[string]StepState)
	}
	if ss, ok := rs.Steps[stepName]; ok {
		return ss.ID, nil
	}
	id := fmt.Sprintf("%s-%s", runID, stepName)
	rs.Steps[stepName] = StepState{ID: id, Name: stepName}
	return id, s.save(rs)
}
