# Dagu Step Commands Implementation Plan

> **For Claude:** Use the `executing-plans` skill to implement this plan task-by-task.

**Goal:** Add `no-mistakes prepare`, `no-mistakes step <name>`, and `no-mistakes pipeline` commands so the workflow can be driven by Dagu (or any external orchestrator) without the push-gate/daemon flow.

**Architecture:** A new `internal/state` package replaces SQLite for the dagu-driven path, storing run state in a JSON file at `$NM_RUN_DIR/state.json`. `StepContext` gets an optional `State state.Store` field; steps prefer it over `DB` when set. Three new cobra commands wire everything together. The existing push-gate flow is untouched.

**Tech Stack:** Go 1.25, cobra, existing `internal/pipeline`, `internal/git`, `internal/config`, `internal/db` packages.

---

## Task 1: `internal/state` package — data types

**Files:**
- Create: `internal/state/state.go`

**Step 1: Write the file**

```go
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
	ID                 string     `json:"id"`
	Round              int        `json:"round"`
	Trigger            string     `json:"trigger"`
	FindingsJSON       string     `json:"findings_json,omitempty"`
	UserFindingsJSON   string     `json:"user_findings_json,omitempty"`
	SelectedFindingIDs string     `json:"selected_finding_ids,omitempty"`
	SelectionSource    string     `json:"selection_source,omitempty"`
	FixSummary         *string    `json:"fix_summary,omitempty"`
	DurationMS         int64      `json:"duration_ms,omitempty"`
	CreatedAt          int64      `json:"created_at"`
}

// StepState holds per-step results for a run.
type StepState struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Status   string  `json:"status,omitempty"`
	Rounds   []Round `json:"rounds,omitempty"`
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
// It mirrors the six DB methods actually called by pipeline steps.
type Store interface {
	GetRun(id string) (*RunState, error)
	UpdateRunHeadSHA(runID, sha string) error
	UpdateRunIntent(runID string, intent RunIntent) error
	UpdateRunPRURL(runID, url string) error
	GetStepsByRun(runID string) ([]StepState, error)
	GetRoundsByStep(stepID string) ([]Round, error)
	AppendRound(stepID string, round Round) error
	EnsureStep(runID, stepName string) (string, error) // returns step ID
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

// DefaultPath returns $NM_RUN_DIR/state.json or falls back to a temp dir.
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
```

**Step 2: Verify it compiles**

```bash
cd /mnt/custom-file-systems/efs/fs-04bf86d02daf87e14/src/no-mistakes
go build ./internal/state/...
```

Expected: no output (clean build).

**Step 3: Commit**

```bash
git add internal/state/state.go
git commit -m "feat(state): add JSON file-backed run state store"
```

---

## Task 2: `internal/state` tests

**Files:**
- Create: `internal/state/state_test.go`

**Step 1: Write tests**

```go
package state_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/state"
)

func newStore(t *testing.T) (*state.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	rs := &state.RunState{
		RunID:         "run-1",
		RepoDir:       "/tmp/repo",
		Branch:        "feature/x",
		BaseSHA:       "aaa",
		HeadSHA:       "bbb",
		DefaultBranch: "main",
		UpstreamURL:   "git@github.com:org/repo.git",
	}
	data, _ := json.MarshalIndent(rs, "", "  ")
	os.WriteFile(path, data, 0o644)
	return state.New(path), "run-1"
}

func TestGetRun(t *testing.T) {
	s, runID := newStore(t)
	rs, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Branch != "feature/x" {
		t.Errorf("branch = %q", rs.Branch)
	}
}

func TestUpdateRunHeadSHA(t *testing.T) {
	s, runID := newStore(t)
	if err := s.UpdateRunHeadSHA(runID, "ccc"); err != nil {
		t.Fatal(err)
	}
	rs, _ := s.GetRun(runID)
	if rs.HeadSHA != "ccc" {
		t.Errorf("head_sha = %q, want ccc", rs.HeadSHA)
	}
}

func TestUpdateRunPRURL(t *testing.T) {
	s, runID := newStore(t)
	if err := s.UpdateRunPRURL(runID, "https://github.com/org/repo/pull/1"); err != nil {
		t.Fatal(err)
	}
	rs, _ := s.GetRun(runID)
	if rs.PRURL == nil || *rs.PRURL != "https://github.com/org/repo/pull/1" {
		t.Errorf("pr_url = %v", rs.PRURL)
	}
}

func TestEnsureStepAndRounds(t *testing.T) {
	s, runID := newStore(t)
	id, err := s.EnsureStep(runID, "review")
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty step id")
	}
	// idempotent
	id2, err := s.EnsureStep(runID, "review")
	if err != nil {
		t.Fatal(err)
	}
	if id != id2 {
		t.Errorf("id changed: %s vs %s", id, id2)
	}

	round := state.Round{Trigger: "initial", FindingsJSON: `{"items":[]}`}
	if err := s.AppendRound(id, round); err != nil {
		t.Fatal(err)
	}
	rounds, err := s.GetRoundsByStep(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 1 {
		t.Errorf("rounds = %d, want 1", len(rounds))
	}
}
```

**Step 2: Run tests**

```bash
cd /mnt/custom-file-systems/efs/fs-04bf86d02daf87e14/src/no-mistakes
go test ./internal/state/... -v
```

Expected: all PASS.

**Step 3: Commit**

```bash
git add internal/state/state_test.go
git commit -m "test(state): add FileStore tests"
```

---

## Task 3: Wire `State` into `StepContext`

**Files:**
- Modify: `internal/pipeline/pipeline.go`
- Modify: `internal/pipeline/steps/round_history.go`
- Modify: `internal/pipeline/steps/common_fix.go`
- Modify: `internal/pipeline/steps/pr.go`
- Modify: `internal/pipeline/steps/push.go`
- Modify: `internal/pipeline/steps/rebase.go`
- Modify: `internal/pipeline/steps/ci.go`
- Modify: `internal/pipeline/steps/intent.go`

**Step 1: Add `State` field to `StepContext`**

In `internal/pipeline/pipeline.go`, add the import and field:

```go
import (
    // existing imports...
    "github.com/kunchenguid/no-mistakes/internal/state"
)

type StepContext struct {
    // ... existing fields unchanged ...

    // State is the JSON file-backed store for dagu-driven runs.
    // When non-nil, steps use it instead of DB for run state mutations.
    // DB remains set for the existing push-gate path.
    State state.Store
}
```

**Step 2: Update `round_history.go` to prefer `State`**

Replace the guard at the top of `roundHistoryPromptSection`:

```go
func roundHistoryPromptSection(sctx *pipeline.StepContext) string {
    if sctx == nil || sctx.StepResultID == "" {
        return ""
    }
    var rounds []roundProvider
    if sctx.State != nil {
        rs, err := sctx.State.GetRoundsByStep(sctx.StepResultID)
        if err != nil || len(rs) == 0 {
            return ""
        }
        // convert state.Round → db.StepRound for renderRoundHistoryEntry
        rounds = stateRoundsToDBRounds(rs)
    } else if sctx.DB != nil {
        dbRounds, err := sctx.DB.GetRoundsByStep(sctx.StepResultID)
        if err != nil || len(dbRounds) == 0 {
            return ""
        }
        rounds = dbRoundsToProvider(dbRounds)
    } else {
        return ""
    }
    // ... rest unchanged
}
```

> Note: the exact adapter pattern will depend on the renderRoundHistoryEntry signature. The key invariant is: `sctx.State != nil` takes priority; `sctx.DB != nil` is the fallback. Same pattern applies to every other DB call site.

**Step 3: Update remaining DB call sites with `state`-first pattern**

For each method call on `sctx.DB` in steps:

| File | Call | State equivalent |
|---|---|---|
| `intent.go` | `sctx.DB.UpdateRunIntent(...)` | `sctx.State.UpdateRunIntent(...)` |
| `push.go` | `sctx.DB.UpdateRunHeadSHA(...)` | `sctx.State.UpdateRunHeadSHA(...)` |
| `rebase.go` | `sctx.DB.UpdateRunHeadSHA(...)` | `sctx.State.UpdateRunHeadSHA(...)` |
| `pr.go` | `sctx.DB.UpdateRunPRURL(...)`, `sctx.DB.GetStepsByRun(...)` | `sctx.State.UpdateRunPRURL(...)`, `sctx.State.GetStepsByRun(...)` |
| `ci.go` | `sctx.DB.GetRun(...)` | `sctx.State.GetRun(...)` |
| `common_fix.go` | `sctx.DB.UpdateRunHeadSHA(...)` | `sctx.State.UpdateRunHeadSHA(...)` |

Pattern for each:

```go
// before
if err := sctx.DB.UpdateRunHeadSHA(sctx.Run.ID, headSHA); err != nil {

// after
if sctx.State != nil {
    if err := sctx.State.UpdateRunHeadSHA(sctx.Run.ID, headSHA); err != nil {
        return err
    }
} else if sctx.DB != nil {
    if err := sctx.DB.UpdateRunHeadSHA(sctx.Run.ID, headSHA); err != nil {
        return err
    }
}
```

**Step 4: Build to verify no compilation errors**

```bash
cd /mnt/custom-file-systems/efs/fs-04bf86d02daf87e14/src/no-mistakes
go build ./...
```

Expected: clean.

**Step 5: Run existing tests to verify no regressions**

```bash
go test -race ./internal/pipeline/... ./internal/pipeline/steps/...
```

Expected: all PASS (State field is nil in all existing tests, so fallback to DB path fires).

**Step 6: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/steps/
git commit -m "feat(pipeline): add optional State store to StepContext"
```

---

## Task 4: `no-mistakes prepare` command

**Files:**
- Create: `internal/cli/prepare_cmd.go`

**Step 1: Write the command**

```go
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/state"
	"github.com/spf13/cobra"
)

func newPrepareCmd() *cobra.Command {
	var repoDir string
	var branch string
	var runDir string

	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "Initialize a run context for dagu-driven execution",
		Long: `prepare resolves the repo state, creates an isolated git worktree,
and writes state.json to NM_RUN_DIR (or --run-dir).

It prints the run ID to stdout so dagu can capture it as an output variable.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Resolve repo dir
			if repoDir == "" {
				var err error
				repoDir, err = os.Getwd()
				if err != nil {
					return fmt.Errorf("get working dir: %w", err)
				}
			}
			absRepo, err := filepath.Abs(repoDir)
			if err != nil {
				return fmt.Errorf("resolve repo dir: %w", err)
			}

			// Fetch origin to ensure base SHA is current
			fmt.Fprintln(cmd.ErrOrStderr(), "fetching origin...")
			defaultBranch := git.DefaultBranch(ctx, absRepo, "origin")
			if err := git.FetchRemoteBranch(ctx, absRepo, "origin", defaultBranch); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: fetch failed: %v\n", err)
			}

			// Resolve branch
			if branch == "" {
				branch, err = git.CurrentBranch(ctx, absRepo)
				if err != nil {
					return fmt.Errorf("resolve branch: %w", err)
				}
				branch = strings.TrimPrefix(branch, "refs/heads/")
			}

			// Resolve SHAs
			headSHA, err := git.HeadSHA(ctx, absRepo)
			if err != nil {
				return fmt.Errorf("resolve HEAD: %w", err)
			}
			headSHA = strings.TrimSpace(headSHA)

			baseSHA := resolveBranchBaseSHAForPrepare(ctx, absRepo, defaultBranch)

			// Resolve upstream URL
			upstreamURL, _ := git.Run(ctx, absRepo, "remote", "get-url", "origin")
			upstreamURL = strings.TrimSpace(upstreamURL)

			// Generate run ID
			runID := fmt.Sprintf("nm-%d", time.Now().UnixMilli())

			// Resolve run dir
			if runDir == "" {
				runDir = os.Getenv("NM_RUN_DIR")
			}
			if runDir == "" {
				p, err := paths.New()
				if err != nil {
					return err
				}
				runDir = p.RunLogDir(runID)
			}
			if err := os.MkdirAll(runDir, 0o755); err != nil {
				return fmt.Errorf("create run dir: %w", err)
			}

			// Create worktree
			p, err := paths.New()
			if err != nil {
				return err
			}
			wtDir := p.WorktreeDir("direct", runID)
			if err := os.MkdirAll(filepath.Dir(wtDir), 0o755); err != nil {
				return fmt.Errorf("create worktrees dir: %w", err)
			}
			if err := git.WorktreeAdd(ctx, absRepo, wtDir, headSHA); err != nil {
				return fmt.Errorf("create worktree: %w", err)
			}
			if err := git.CopyLocalUserIdentity(ctx, absRepo, wtDir); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: copy git identity: %v\n", err)
			}

			// Write state.json
			statePath := filepath.Join(runDir, "state.json")
			rs := state.RunState{
				RunID:         runID,
				RepoDir:       absRepo,
				Branch:        branch,
				BaseSHA:       baseSHA,
				HeadSHA:       headSHA,
				DefaultBranch: defaultBranch,
				UpstreamURL:   upstreamURL,
				WorktreeDir:   wtDir,
			}
			data, err := json.MarshalIndent(rs, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal state: %w", err)
			}
			if err := os.WriteFile(statePath, data, 0o644); err != nil {
				return fmt.Errorf("write state: %w", err)
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "prepared run %s\n  worktree: %s\n  state:    %s\n", runID, wtDir, statePath)

			// Print run ID to stdout for dagu output capture
			fmt.Println(runID)
			return nil
		},
	}

	cmd.Flags().StringVar(&repoDir, "repo", "", "path to git repo (default: current directory)")
	cmd.Flags().StringVar(&branch, "branch", "", "branch name (default: current branch)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "directory for state.json (default: $NM_RUN_DIR or ~/.no-mistakes/logs/<run-id>)")

	return cmd
}

// resolveBranchBaseSHAForPrepare returns the merge-base of HEAD against the
// default branch. Falls back to the empty-tree SHA on any error.
func resolveBranchBaseSHAForPrepare(ctx context.Context, repoDir, defaultBranch string) string {
	mb, err := git.Run(ctx, repoDir, "merge-base", "HEAD", "origin/"+defaultBranch)
	if err == nil && strings.TrimSpace(mb) != "" {
		return strings.TrimSpace(mb)
	}
	return git.EmptyTreeSHA
}
```

> Note: add `WorktreeDir string` to `state.RunState` in Task 1's struct.

**Step 2: Register in root**

In `internal/cli/root.go`, add to `newRootCmd()`:

```go
cmd.AddCommand(newPrepareCmd())
```

**Step 3: Build**

```bash
go build ./...
```

**Step 4: Smoke test**

```bash
cd /tmp && mkdir test-repo && cd test-repo
git init && git commit --allow-empty -m "init"
no-mistakes prepare --repo /tmp/test-repo
```

Expected: prints a run ID like `nm-1234567890`, creates `state.json` in run dir.

**Step 5: Commit**

```bash
git add internal/cli/prepare_cmd.go internal/cli/root.go
git commit -m "feat(cli): add prepare command for dagu-driven runs"
```

---

## Task 5: `no-mistakes step <name>` command

**Files:**
- Create: `internal/cli/step_cmd.go`

**Step 1: Write the command**

```go
package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/pipeline/steps"
	"github.com/kunchenguid/no-mistakes/internal/state"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/spf13/cobra"
)

func newStepCmd() *cobra.Command {
	var runID string
	var runDir string

	cmd := &cobra.Command{
		Use:   "step <name>",
		Short: "Run a single pipeline step against an existing run",
		Long: `step runs one named pipeline step (intent, rebase, review, test,
document, lint, push, pr, ci) against a run previously created by prepare.

Exits 0 on success or skip, 1 on blocking findings.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			stepName := types.StepName(args[0])

			// Validate step name
			step := findStep(stepName)
			if step == nil {
				return fmt.Errorf("unknown step %q — valid: %s", stepName, validStepNames())
			}

			// Locate state file
			statePath, err := resolveStatePath(runID, runDir)
			if err != nil {
				return err
			}

			// Load state
			store := state.New(statePath)
			rs, err := store.GetRun(runID)
			if err != nil {
				return fmt.Errorf("load run state: %w", err)
			}

			// Ensure step record exists, get its ID
			stepResultID, err := store.EnsureStep(rs.RunID, string(stepName))
			if err != nil {
				return fmt.Errorf("ensure step: %w", err)
			}

			// Load config
			cfg, err := loadConfigForRun(rs)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// Build agent
			ag, err := agent.New(cfg)
			if err != nil {
				return fmt.Errorf("build agent: %w", err)
			}
			defer ag.Close()

			// Build a minimal db.Run from state for StepContext compatibility
			run := stateToDBRun(rs)
			repo := stateToDBRepo(rs)

			// Build StepContext
			p, _ := paths.New()
			logDir := p.RunLogDir(rs.RunID)
			os.MkdirAll(logDir, 0o755)
			logPath := filepath.Join(logDir, string(stepName)+".log")
			logFile, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if logFile != nil {
				defer logFile.Close()
			}

			sctx := &pipeline.StepContext{
				Ctx:          ctx,
				Run:          run,
				Repo:         repo,
				WorkDir:      rs.WorktreeDir,
				Agent:        ag,
				Config:       cfg,
				State:        store,
				StepResultID: stepResultID,
				Log: func(msg string) {
					fmt.Fprintln(cmd.ErrOrStderr(), msg)
					if logFile != nil {
						fmt.Fprintln(logFile, msg)
					}
				},
				LogChunk: func(chunk string) {
					fmt.Fprint(cmd.ErrOrStderr(), chunk)
				},
			}

			// Propagate intent from state
			if rs.Intent != nil {
				sctx.UserIntent = *rs.Intent
				run.Intent = rs.Intent
				run.IntentSource = rs.IntentSource
				run.IntentScore = rs.IntentScore
			}

			// Execute step
			outcome, err := step.Execute(sctx)
			if err != nil {
				return fmt.Errorf("step %s: %w", stepName, err)
			}

			// Persist any head SHA changes back to state
			if run.HeadSHA != rs.HeadSHA {
				_ = store.UpdateRunHeadSHA(rs.RunID, run.HeadSHA)
			}

			if outcome.Skipped {
				fmt.Fprintf(cmd.ErrOrStderr(), "step %s: skipped\n", stepName)
				return nil
			}
			if outcome.NeedsApproval || outcome.ExitCode != 0 {
				fmt.Fprintf(cmd.ErrOrStderr(), "step %s: blocking findings\n", stepName)
				if outcome.Findings != "" {
					fmt.Fprintln(cmd.ErrOrStderr(), outcome.Findings)
				}
				return &exitError{code: 1}
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "step %s: ok\n", stepName)
			return nil
		},
	}

	cmd.Flags().StringVar(&runID, "run-id", "", "run ID from prepare (required, or set NM_RUN_ID env)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "run dir containing state.json (default: $NM_RUN_DIR)")

	return cmd
}

// findStep returns the pipeline.Step for a given name, or nil.
func findStep(name types.StepName) pipeline.Step {
	for _, s := range steps.AllSteps() {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func validStepNames() string {
	all := types.AllSteps()
	names := make([]string, len(all))
	for i, s := range all {
		names[i] = string(s)
	}
	return strings.Join(names, ", ")
}

func resolveStatePath(runID, runDir string) (string, error) {
	if runID == "" {
		runID = os.Getenv("NM_RUN_ID")
	}
	if runDir == "" {
		runDir = os.Getenv("NM_RUN_DIR")
	}
	if runDir != "" {
		return filepath.Join(runDir, "state.json"), nil
	}
	if runID != "" {
		p, err := paths.New()
		if err != nil {
			return "", err
		}
		return filepath.Join(p.RunLogDir(runID), "state.json"), nil
	}
	return "", fmt.Errorf("provide --run-id or set NM_RUN_ID / NM_RUN_DIR")
}

func loadConfigForRun(rs *state.RunState) (*config.Config, error) {
	global, err := config.LoadGlobal()
	if err != nil {
		return nil, err
	}
	repo, err := config.LoadRepo(rs.WorktreeDir)
	if err != nil {
		repo = &config.RepoConfig{}
	}
	return config.Merge(global, repo), nil
}

// stateToDBRun builds a minimal db.Run from RunState for StepContext.
// Steps read HeadSHA, Branch, BaseSHA, Intent from this struct.
func stateToDBRun(rs *state.RunState) *db.Run {
	return &db.Run{
		ID:           rs.RunID,
		Branch:       rs.Branch,
		HeadSHA:      rs.HeadSHA,
		BaseSHA:      rs.BaseSHA,
		Intent:       rs.Intent,
		IntentSource: rs.IntentSource,
		IntentScore:  rs.IntentScore,
		PRURL:        rs.PRURL,
	}
}

// stateToDBRepo builds a minimal db.Repo from RunState for StepContext.
func stateToDBRepo(rs *state.RunState) *db.Repo {
	return &db.Repo{
		WorkingPath:   rs.RepoDir,
		UpstreamURL:   rs.UpstreamURL,
		DefaultBranch: rs.DefaultBranch,
	}
}
```

**Step 2: Register in root**

```go
cmd.AddCommand(newStepCmd())
```

**Step 3: Build**

```bash
go build ./...
```

**Step 4: Verify help output**

```bash
no-mistakes step --help
no-mistakes step review --help
```

**Step 5: Commit**

```bash
git add internal/cli/step_cmd.go internal/cli/root.go
git commit -m "feat(cli): add step command for single-step dagu execution"
```

---

## Task 6: `no-mistakes pipeline` command

**Files:**
- Create: `internal/cli/pipeline_cmd.go`

**Step 1: Write the command**

```go
package cli

import (
	"fmt"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/types"
	"github.com/spf13/cobra"
)

func newPipelineCmd() *cobra.Command {
	var runID string
	var runDir string
	var stepsFlag string

	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Run all (or selected) pipeline steps sequentially",
		Long: `pipeline runs steps in declared order against an existing run.
Use --steps to run a subset: --steps review,lint,push`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Resolve which steps to run
			var toRun []types.StepName
			if stepsFlag != "" {
				for _, part := range strings.Split(stepsFlag, ",") {
					name := types.StepName(strings.TrimSpace(part))
					if findStep(name) == nil {
						return fmt.Errorf("unknown step %q", name)
					}
					toRun = append(toRun, name)
				}
			} else {
				toRun = types.AllSteps()
			}

			// Run each step in order by delegating to step command logic
			statePath, err := resolveStatePath(runID, runDir)
			if err != nil {
				return err
			}

			for _, name := range toRun {
				fmt.Fprintf(cmd.ErrOrStderr(), "\n=== step: %s ===\n", name)
				if err := runOneStep(ctx, cmd, name, statePath); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&runID, "run-id", "", "run ID (or set NM_RUN_ID)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "run dir (or set NM_RUN_DIR)")
	cmd.Flags().StringVar(&stepsFlag, "steps", "", "comma-separated step names to run (default: all)")

	return cmd
}
```

> `runOneStep` extracts the core logic from `newStepCmd` into a shared function — refactor step_cmd.go to expose it.

**Step 2: Register in root**

```go
cmd.AddCommand(newPipelineCmd())
```

**Step 3: Build and test help**

```bash
go build ./...
no-mistakes pipeline --help
```

**Step 4: Commit**

```bash
git add internal/cli/pipeline_cmd.go internal/cli/root.go
git commit -m "feat(cli): add pipeline command for sequential step execution"
```

---

## Task 7: Dagu DAG definition

**Files:**
- Create: `dags/no-mistakes.yaml`

**Step 1: Write the DAG**

```yaml
# no-mistakes.yaml — dagu DAG for on-demand pipeline runs.
#
# Usage:
#   dagu start dags/no-mistakes.yaml -- REPO_DIR=/path/to/repo BRANCH=feature/x
#
# Customization:
#   - Comment out steps to skip them
#   - Reorder steps by changing depends: fields
#   - Add new steps: implement pipeline.Step, register in AllSteps(), add a node here

params:
  - REPO_DIR: ""    # path to git repo; defaults to current dir if empty
  - BRANCH: ""      # branch name; defaults to current branch if empty

env:
  - NM_RUN_DIR: ${DAGU_RUN_DIR}

steps:
  - name: prepare
    command: no-mistakes prepare --repo "${REPO_DIR}" --branch "${BRANCH}"
    output: RUN_ID

  - name: intent
    command: no-mistakes step intent --run-id ${RUN_ID}
    depends:
      - prepare
    continueOn:
      skipped: true

  - name: rebase
    command: no-mistakes step rebase --run-id ${RUN_ID}
    depends:
      - intent
    continueOn:
      skipped: true

  - name: review
    command: no-mistakes step review --run-id ${RUN_ID}
    depends:
      - rebase

  - name: test
    command: no-mistakes step test --run-id ${RUN_ID}
    depends:
      - review

  - name: document
    command: no-mistakes step document --run-id ${RUN_ID}
    depends:
      - test
    continueOn:
      skipped: true

  - name: lint
    command: no-mistakes step lint --run-id ${RUN_ID}
    depends:
      - document

  - name: push
    command: no-mistakes step push --run-id ${RUN_ID}
    depends:
      - lint

  - name: pr
    command: no-mistakes step pr --run-id ${RUN_ID}
    depends:
      - push
    continueOn:
      skipped: true

  - name: ci
    command: no-mistakes step ci --run-id ${RUN_ID}
    depends:
      - pr
    continueOn:
      skipped: true
```

**Step 2: Verify dagu can parse it**

```bash
dagu dry dags/no-mistakes.yaml -- REPO_DIR=/tmp BRANCH=main
```

Expected: dry run output showing all nodes, no parse errors.

**Step 3: Commit**

```bash
git add dags/no-mistakes.yaml
git commit -m "feat(dags): add dagu DAG for on-demand pipeline runs"
```

---

## Task 8: Integration smoke test

**Step 1: Build final binary**

```bash
cd /mnt/custom-file-systems/efs/fs-04bf86d02daf87e14/src/no-mistakes
make build
```

**Step 2: Run full test suite**

```bash
go test -race ./...
```

Expected: all PASS. Existing push-gate tests unaffected (State is nil, DB path fires).

**Step 3: Smoke test prepare + step**

```bash
# use the no-mistakes repo itself as the target
export NM_RUN_DIR=$(mktemp -d)
RUN_ID=$(no-mistakes prepare --repo . --branch $(git branch --show-current))
echo "run: $RUN_ID"
no-mistakes step intent --run-id $RUN_ID
no-mistakes step review --run-id $RUN_ID
```

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: dagu-driven pipeline smoke tested"
```

---

## Summary

| Command | Purpose |
|---|---|
| `no-mistakes prepare` | Creates worktree + `state.json`, prints run ID |
| `no-mistakes step <name>` | Runs one step against existing run, exits 0/1 |
| `no-mistakes pipeline` | Runs all (or subset) steps sequentially |
| `dags/no-mistakes.yaml` | Dagu DAG: each node calls `no-mistakes step` |

**Nothing removed.** Push-gate, daemon, TUI all continue working. The new path is additive.
