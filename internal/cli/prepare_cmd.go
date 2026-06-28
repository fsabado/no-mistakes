package cli

import (
	"context"
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

It prints the run ID to stdout so dagu can capture it as an output variable:

  steps:
    - name: prepare
      command: no-mistakes prepare --repo $REPO_DIR --branch $BRANCH
      output: RUN_ID`,
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
			defaultBranch := git.DefaultBranch(ctx, absRepo, "origin")
			fmt.Fprintf(cmd.ErrOrStderr(), "fetching origin/%s...\n", defaultBranch)
			if err := git.FetchRemoteBranch(ctx, absRepo, "origin", defaultBranch); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: fetch failed: %v\n", err)
			}

			// Resolve branch
			if branch == "" {
				b, err := git.CurrentBranch(ctx, absRepo)
				if err != nil {
					return fmt.Errorf("resolve branch: %w", err)
				}
				branch = strings.TrimPrefix(strings.TrimSpace(b), "refs/heads/")
			}

			// Resolve SHAs
			rawHead, err := git.HeadSHA(ctx, absRepo)
			if err != nil {
				return fmt.Errorf("resolve HEAD: %w", err)
			}
			headSHA := strings.TrimSpace(rawHead)

			baseSHA := resolvePrepareBaseSHA(ctx, absRepo, defaultBranch)

			// Resolve upstream URL
			upstreamURL, _ := git.Run(ctx, absRepo, "remote", "get-url", "origin")
			upstreamURL = strings.TrimSpace(upstreamURL)

			// Generate run ID
			runID := fmt.Sprintf("nm-%d", time.Now().UnixMilli())

			// Resolve run dir
			if runDir == "" {
				runDir = os.Getenv("NM_RUN_DIR")
			}

			p, err := paths.New()
			if err != nil {
				return err
			}

			if runDir == "" {
				runDir = p.RunLogDir(runID)
			}
			if err := os.MkdirAll(runDir, 0o755); err != nil {
				return fmt.Errorf("create run dir: %w", err)
			}

			// Create isolated worktree from the working repo
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
				WorktreeDir:   wtDir,
				Branch:        branch,
				BaseSHA:       baseSHA,
				HeadSHA:       headSHA,
				DefaultBranch: defaultBranch,
				UpstreamURL:   upstreamURL,
			}
			data, err := json.MarshalIndent(rs, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal state: %w", err)
			}
			if err := os.WriteFile(statePath, data, 0o644); err != nil {
				return fmt.Errorf("write state: %w", err)
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "prepared run %s\n  worktree: %s\n  state:    %s\n", runID, wtDir, statePath)

			// Print run ID to stdout — dagu captures this as an output variable
			fmt.Println(runID)
			return nil
		},
	}

	cmd.Flags().StringVar(&repoDir, "repo", "", "path to git repo (default: current directory)")
	cmd.Flags().StringVar(&branch, "branch", "", "branch name (default: current branch)")
	cmd.Flags().StringVar(&runDir, "run-dir", "", "directory for state.json (default: $NM_RUN_DIR or ~/.no-mistakes/logs/<run-id>)")

	return cmd
}

// resolvePrepareBaseSHA returns the merge-base of HEAD against the default
// branch remote tip. Falls back to the empty-tree SHA on any error.
func resolvePrepareBaseSHA(ctx context.Context, repoDir, defaultBranch string) string {
	mb, err := git.Run(ctx, repoDir, "merge-base", "HEAD", "origin/"+defaultBranch)
	if err == nil && strings.TrimSpace(mb) != "" {
		return strings.TrimSpace(mb)
	}
	return git.EmptyTreeSHA
}
