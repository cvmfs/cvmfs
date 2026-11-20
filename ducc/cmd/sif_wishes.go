package cmd

import (
	"context"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

func convertSIFWishes(ctx context.Context, recipe lib.Recipe, force bool, between func()) (lib.ConversionSummary, []string) {
	if skipFlat && len(recipe.SIFWishes) > 0 {
		l.Log().WithField("entries", len(recipe.SIFWishes)).Info("--skip-flat given, not converting sif_input")
		return lib.ConversionSummary{}, nil
	}
	ignored := func(source string) bool { return isInIgnoreList(source, recipe.IgnoreErrorsList) }
	return lib.ConvertSIFWishes(ctx, recipe, force, ignored, between)
}
