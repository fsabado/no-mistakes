package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
		Long: `step runs one named pipeline step against a run created by prepare.

Valid step names: intent, rebase, review, test, document, lint, push, pr, ci

Exits 0 on success or skip, 1 on blocking findings.

Example:
  RUN_ID=$(no-mistakes prepare --repo /path/to/repo)
  no-mistakes step review --run-id $RUN_ID`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			stepName := types.StepName(args[0])

			// Validate step name
			step := findStep(stepName)
			if step == nil {
				return fmt.Errorf("unknown step %q — valid: %s", stepName, validStepNames())
			}

			// Locate and load state
			statePath, resolvedRunID, err := resolveStatePathAndRunID(runID, runDir)
			if err != nil {
				return err
			}

			store := state.New(statePath)
			rs, err := store.GetRun(resolvedRunID)
			if err != nil {
				return fmt.Errorf("load run state: %w", err)
			}

			// Ensure step record exists
			stepResultID, err := store.EnsureStep(rs.RunID, string(stepName))
			if err != nil {
				return fmt.Errorf("ensure step: %w", err)
			}

			// Load config from worktree
			cfg, err := loadStepConfig(rs)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// Build agent
			ag, err := agent.New(cfg.Agent, cfg.AgentPath(), cfg.AgentArgs())
			if err != nil {
				return fmt.Errorf("build agent: %w", err)
			}
			defer ag.Close()

			// Build minimal db.Run and db.Repo for StepContext compatibility
			run := stateRunToDBRun(rs)
			repo := stateRunToDBRepo(rs)

			// Set up logging
			p, _ := paths.New()
			logDir := p.RunLogDir(rs.RunID)
			os.MkdirAll(logDir, 0o755)
			logPath := filepath.Join(logDir, string(stepName)+".log")
			logFile, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if logFile != nil {
				defer logFile.Close()
			}

			logFn := func(msg string) {
				fmt.Fprintln(cmd.ErrOrStderr(), msg)
				if logFile != nil {
					fmt.Fprintln(logFile, msg)
				}
			}
			logChunkFn := func(chunk string) {
				fmt.Fprint(cmd.ErrOrStderr(), chunk)
				if logFile != nil {
					fmt.Fprint(logFile, chunk)
				}
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
				Log:          logFn,
				LogChunk:     logChunkFn,
				LogFile:      logFn,
			}

			// Propagate intent
			if rs.Intent != nil {
				sctx.UserIntent = *rs.Intent
			}

			// Execute
			outcome, err := step.Execute(sctx)
			if err != nil {
				return fmt.Errorf("step %s: %w", stepName, err)
			}

			// Persist head SHA changes (agent may have committed)
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

	cmd.Flags().StringVar(&runID, "run-id", "", "run ID from prepare (or set NM_RUN_ID env)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "directory containing state.json (or set NM_RUN_DIR)")

	return cmd
}

// findStep returns the pipeline.Step matching name, or nil.
func findStep(name types.StepName) pipeline.Step {
	for _, s := range steps.AllSteps() {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// validStepNames returns a comma-separated list of all valid step names.
func validStepNames() string {
	all := types.AllSteps()
	names := make([]string, len(all))
	for i, s := range all {
		names[i] = string(s)
	}
	return strings.Join(names, ", ")
}

// resolveStatePathAndRunID returns the state.json path and run ID from flags/env.
func resolveStatePathAndRunID(runID, runDir string) (string, string, error) {
	if runID == "" {
		runID = os.Getenv("NM_RUN_ID")
	}
	if runDir == "" {
		runDir = os.Getenv("NM_RUN_DIR")
	}
	if runDir != "" {
		statePath := filepath.Join(runDir, "state.json")
		if runID == "" {
			// Derive run ID from state file
			data, err := os.ReadFile(statePath)
			if err != nil {
				return "", "", fmt.Errorf("read state file: %w", err)
			}
			var rs state.RunState
			if err := json.Unmarshal(data, &rs); err != nil {
				return "", "", fmt.Errorf("parse state file: %w", err)
			}
			runID = rs.RunID
		}
		return statePath, runID, nil
	}
	if runID != "" {
		p, err := paths.New()
		if err != nil {
			return "", "", err
		}
		return filepath.Join(p.RunLogDir(runID), "state.json"), runID, nil
	}
	return "", "", fmt.Errorf("provide --run-id or set NM_RUN_ID / NM_RUN_DIR")
}

// loadStepConfig loads merged global + repo config for a run.
func loadStepConfig(rs *state.RunState) (*config.Config, error) {
	p, err := paths.New()
	if err != nil {
		return nil, err
	}
	global, err := config.LoadGlobal(p.ConfigFile())
	if err != nil {
		global = &config.GlobalConfig{}
	}
	repo, err := config.LoadRepo(rs.WorktreeDir)
	if err != nil {
		repo = &config.RepoConfig{}
	}
	return config.Merge(global, repo), nil
}

// stateRunToDBRun builds a minimal db.Run from RunState for StepContext.
func stateRunToDBRun(rs *state.RunState) *db.Run {
	return &db.Run{
		ID:           rs.RunID,
		Branch:       rs.Branch,
		HeadSHA:      rs.HeadSHA,
		BaseSHA:      rs.BaseSHA,
		Intent:       rs.Intent,
		IntentSource: rs.IntentSource,
		PRURL:        rs.PRURL,
	}
}

// stateRunToDBRepo builds a minimal db.Repo from RunState for StepContext.
func stateRunToDBRepo(rs *state.RunState) *db.Repo {
	return &db.Repo{
		WorkingPath:   rs.RepoDir,
		UpstreamURL:   rs.UpstreamURL,
		DefaultBranch: rs.DefaultBranch,
	}
}
