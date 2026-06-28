package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/state"
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

Use --steps to run a custom subset:
  no-mistakes pipeline --run-id <id> --steps review,lint,push

Steps not listed are skipped. Order follows the canonical pipeline order
regardless of the order specified in --steps.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Resolve which steps to run, in canonical order
			var toRun []types.StepName
			if stepsFlag != "" {
				requested := make(map[types.StepName]bool)
				for _, part := range strings.Split(stepsFlag, ",") {
					name := types.StepName(strings.TrimSpace(part))
					if findStep(name) == nil {
						return fmt.Errorf("unknown step %q — valid: %s", name, validStepNames())
					}
					requested[name] = true
				}
				for _, s := range types.AllSteps() {
					if requested[s] {
						toRun = append(toRun, s)
					}
				}
			} else {
				toRun = types.AllSteps()
			}

			// Locate state
			statePath, resolvedRunID, err := resolveStatePathAndRunID(runID, runDir)
			if err != nil {
				return err
			}

			store := state.New(statePath)
			rs, err := store.GetRun(resolvedRunID)
			if err != nil {
				return fmt.Errorf("load run state: %w", err)
			}

			// Load config once from worktree
			cfg, err := loadStepConfig(rs)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// Build agent once
			ag, err := agent.New(cfg.Agent, cfg.AgentPath(), cfg.AgentArgs())
			if err != nil {
				return fmt.Errorf("build agent: %w", err)
			}
			defer ag.Close()

			p, _ := paths.New()

			// Run each step sequentially
			for _, name := range toRun {
				fmt.Fprintf(cmd.ErrOrStderr(), "\n=== step: %s ===\n", name)

				step := findStep(name)
				if step == nil {
					return fmt.Errorf("step %q not found", name)
				}

				// Reload state so head SHA updates from prior steps propagate
				rs, err = store.GetRun(resolvedRunID)
				if err != nil {
					return fmt.Errorf("reload state before %s: %w", name, err)
				}

				run := stateRunToDBRun(rs)
				repo := stateRunToDBRepo(rs)

				stepResultID, err := store.EnsureStep(rs.RunID, string(name))
				if err != nil {
					return fmt.Errorf("ensure step %s: %w", name, err)
				}

				logDir := p.RunLogDir(rs.RunID)
				os.MkdirAll(logDir, 0o755)
				logPath := filepath.Join(logDir, string(name)+".log")
				logFile, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)

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
				if rs.Intent != nil {
					sctx.UserIntent = *rs.Intent
				}

				outcome, err := step.Execute(sctx)
				if logFile != nil {
					logFile.Close()
				}
				if err != nil {
					return fmt.Errorf("step %s: %w", name, err)
				}

				// Persist any head SHA changes
				if run.HeadSHA != rs.HeadSHA {
					_ = store.UpdateRunHeadSHA(rs.RunID, run.HeadSHA)
				}

				if outcome.Skipped {
					fmt.Fprintf(cmd.ErrOrStderr(), "step %s: skipped\n", name)
					continue
				}
				if outcome.SkipRemaining {
					fmt.Fprintf(cmd.ErrOrStderr(), "step %s: skip remaining steps\n", name)
					break
				}
				if outcome.NeedsApproval || outcome.ExitCode != 0 {
					fmt.Fprintf(cmd.ErrOrStderr(), "step %s: blocking findings — stopping\n", name)
					if outcome.Findings != "" {
						fmt.Fprintln(cmd.ErrOrStderr(), outcome.Findings)
					}
					return &exitError{code: 1}
				}

				fmt.Fprintf(cmd.ErrOrStderr(), "step %s: ok\n", name)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&runID, "run-id", "", "run ID (or set NM_RUN_ID)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "run dir (or set NM_RUN_DIR)")
	cmd.Flags().StringVar(&stepsFlag, "steps", "", "comma-separated step names (default: all)")

	return cmd
}

// loadStepConfig is defined in step_cmd.go and shared here.
// Declared there: func loadStepConfig(rs *state.RunState) (*config.Config, error)
var _ = (*config.Config)(nil) // ensure config import used
