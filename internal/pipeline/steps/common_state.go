package steps

import (
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/state"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// stateUpdateRunHeadSHA updates the run's head SHA via State (dagu path)
// or DB (push-gate path), whichever is set.
func stateUpdateRunHeadSHA(sctx *pipeline.StepContext, sha string) error {
	if sctx.State != nil {
		return sctx.State.UpdateRunHeadSHA(sctx.Run.ID, sha)
	}
	if sctx.DB != nil {
		return sctx.DB.UpdateRunHeadSHA(sctx.Run.ID, sha)
	}
	return nil
}

// stateUpdateRunPRURL updates the PR URL via State or DB.
func stateUpdateRunPRURL(sctx *pipeline.StepContext, url string) error {
	if sctx.State != nil {
		return sctx.State.UpdateRunPRURL(sctx.Run.ID, url)
	}
	if sctx.DB != nil {
		return sctx.DB.UpdateRunPRURL(sctx.Run.ID, url)
	}
	return nil
}

// stateUpdateRunIntent updates the run intent via State or DB.
func stateUpdateRunIntent(sctx *pipeline.StepContext, si state.RunIntent) error {
	if sctx.State != nil {
		return sctx.State.UpdateRunIntent(sctx.Run.ID, si)
	}
	if sctx.DB != nil {
		return sctx.DB.UpdateRunIntent(sctx.Run.ID, db.RunIntent{
			Summary:   si.Summary,
			Source:    si.Source,
			SessionID: si.SessionID,
			Score:     si.Score,
		})
	}
	return nil
}

// stateGetStepsByRun returns step results via State or DB.
// Returns nil slice (not error) when neither store is available.
func stateGetStepsByRun(sctx *pipeline.StepContext) ([]*db.StepResult, error) {
	if sctx.State != nil {
		ss, err := sctx.State.GetStepsByRun(sctx.Run.ID)
		if err != nil {
			return nil, err
		}
		// Convert state.StepState → db.StepResult for prsummary compatibility
		out := make([]*db.StepResult, 0, len(ss))
		for _, s := range ss {
			s := s // capture
			out = append(out, &db.StepResult{
				ID:       s.ID,
				StepName: types.StepName(s.Name),
			})
		}
		return out, nil
	}
	if sctx.DB != nil {
		return sctx.DB.GetStepsByRun(sctx.Run.ID)
	}
	return nil, nil
}

// stateGetRoundsByStep returns rounds for a step via State or DB.
func stateGetRoundsByStep(sctx *pipeline.StepContext, stepID string) ([]*db.StepRound, error) {
	if sctx.State != nil {
		rs, err := sctx.State.GetRoundsByStep(stepID)
		if err != nil {
			return nil, err
		}
		out := make([]*db.StepRound, 0, len(rs))
		for i := range rs {
			r := &rs[i]
			sr := &db.StepRound{
				ID:         r.ID,
				Round:      r.Round,
				Trigger:    r.Trigger,
				FixSummary: r.FixSummary,
				DurationMS: r.DurationMS,
				CreatedAt:  r.CreatedAt,
			}
			if r.FindingsJSON != "" {
				v := r.FindingsJSON
				sr.FindingsJSON = &v
			}
			if r.UserFindingsJSON != "" {
				v := r.UserFindingsJSON
				sr.UserFindingsJSON = &v
			}
			if r.SelectedFindingIDs != "" {
				v := r.SelectedFindingIDs
				sr.SelectedFindingIDs = &v
			}
			if r.SelectionSource != "" {
				v := r.SelectionSource
				sr.SelectionSource = &v
			}
			out = append(out, sr)
		}
		return out, nil
	}
	if sctx.DB != nil {
		return sctx.DB.GetRoundsByStep(stepID)
	}
	return nil, nil
}

// stateGetRun returns the run record via State or DB.
func stateGetRun(sctx *pipeline.StepContext) (*db.Run, error) {
	if sctx.State != nil {
		rs, err := sctx.State.GetRun(sctx.Run.ID)
		if err != nil {
			return nil, err
		}
		run := *sctx.Run // copy existing run
		run.HeadSHA = rs.HeadSHA
		if rs.PRURL != nil {
			run.PRURL = rs.PRURL
		}
		if rs.Intent != nil {
			run.Intent = rs.Intent
		}
		return &run, nil
	}
	if sctx.DB != nil {
		return sctx.DB.GetRun(sctx.Run.ID)
	}
	return sctx.Run, nil
}
