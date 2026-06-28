package state_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/state"
)

func newStore(t *testing.T) (*state.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	rs := state.RunState{
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
		t.Errorf("branch = %q, want feature/x", rs.Branch)
	}
}

func TestGetRunMismatch(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.GetRun("wrong-id")
	if err == nil {
		t.Fatal("expected error for mismatched run ID")
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

func TestUpdateRunIntent(t *testing.T) {
	s, runID := newStore(t)
	score := 0.9
	intent := state.RunIntent{Summary: "add retry logic", Source: "agent", Score: score}
	if err := s.UpdateRunIntent(runID, intent); err != nil {
		t.Fatal(err)
	}
	rs, _ := s.GetRun(runID)
	if rs.Intent == nil || *rs.Intent != "add retry logic" {
		t.Errorf("intent = %v", rs.Intent)
	}
	if rs.IntentScore == nil || *rs.IntentScore != score {
		t.Errorf("intent_score = %v", rs.IntentScore)
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

func TestEnsureStepIdempotent(t *testing.T) {
	s, runID := newStore(t)
	id1, err := s.EnsureStep(runID, "review")
	if err != nil {
		t.Fatal(err)
	}
	if id1 == "" {
		t.Fatal("empty step id")
	}
	id2, err := s.EnsureStep(runID, "review")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("EnsureStep not idempotent: %s vs %s", id1, id2)
	}
}

func TestGetStepsByRun(t *testing.T) {
	s, runID := newStore(t)
	_, _ = s.EnsureStep(runID, "review")
	_, _ = s.EnsureStep(runID, "lint")
	steps, err := s.GetStepsByRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Errorf("steps = %d, want 2", len(steps))
	}
}

func TestAppendAndGetRounds(t *testing.T) {
	s, runID := newStore(t)
	stepID, err := s.EnsureStep(runID, "review")
	if err != nil {
		t.Fatal(err)
	}

	round := state.Round{Trigger: "initial", FindingsJSON: `{"items":[]}`}
	if err := s.AppendRound(stepID, round); err != nil {
		t.Fatal(err)
	}
	round2 := state.Round{Trigger: "fix", FindingsJSON: `{"items":[]}`}
	if err := s.AppendRound(stepID, round2); err != nil {
		t.Fatal(err)
	}

	rounds, err := s.GetRoundsByStep(stepID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rounds) != 2 {
		t.Errorf("rounds = %d, want 2", len(rounds))
	}
	if rounds[0].Trigger != "initial" {
		t.Errorf("round[0].trigger = %q", rounds[0].Trigger)
	}
	if rounds[1].Trigger != "fix" {
		t.Errorf("round[1].trigger = %q", rounds[1].Trigger)
	}
}

func TestAppendRoundUnknownStep(t *testing.T) {
	s, _ := newStore(t)
	err := s.AppendRound("nonexistent-step", state.Round{Trigger: "x"})
	if err == nil {
		t.Fatal("expected error for unknown step ID")
	}
}

func TestGetRoundsByStepUnknown(t *testing.T) {
	s, _ := newStore(t)
	rounds, err := s.GetRoundsByStep("nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rounds != nil {
		t.Errorf("expected nil rounds for unknown step, got %v", rounds)
	}
}
