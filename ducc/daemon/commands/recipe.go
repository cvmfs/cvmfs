package commands

import (
	"fmt"

	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
)

type ApplyRecipeArgs struct {
	Recipe          string
	WishList        string
	RemoveMissing   bool
	UpdateExisting  bool
	OutputOptions   db.WishOutputOptions
	ScheduleOptions db.WishScheduleOptions
}

type ApplyRecipeResponse struct {
	Created  []db.Wish
	Updated  []db.Wish
	Removed  []db.Wish
	Triggers []db.Trigger
}

func (c *CommandService) ApplyRecipe(args ApplyRecipeArgs, reply *ApplyRecipeResponse) error {
	if args.WishList == "" {
		args.WishList = "default"
	}
	parsedRecipe, err := daemon.ParseYamlRecipeV1([]byte(args.Recipe), args.WishList)
	if err != nil {
		return fmt.Errorf("could not parse recipe: %s", err)
	}

	tx, err := db.GetTransaction()
	if err != nil {
		return fmt.Errorf("could not get db transaction: %s", err)
	}
	defer tx.Rollback()

	newIdentifiers, existingWishes, err := db.FilterNewWishes(tx, parsedRecipe.Wishes)
	if err != nil {
		return fmt.Errorf("could not filter new wishes: %s", err)
	}

	reply.Triggers = make([]db.Trigger, 0)

	// Create new wishes
	toCreate := make([]db.Wish, len(newIdentifiers))
	for i, identifier := range newIdentifiers {
		wish := db.Wish{
			Identifier:      identifier,
			OutputOptions:   args.OutputOptions,
			ScheduleOptions: args.ScheduleOptions,
		}
		toCreate[i] = wish
	}
	reply.Created, err = db.CreateWishes(tx, toCreate, false)
	if err != nil {
		return fmt.Errorf("could not create wishes: %s", err)
	}

	// Update existing wishes
	if args.UpdateExisting {
		toUpdate := make([]db.Wish, len(existingWishes))
		for i, wish := range existingWishes {
			wish.OutputOptions = args.OutputOptions
			wish.ScheduleOptions = args.ScheduleOptions
			toUpdate[i] = wish
		}
		reply.Updated, err = db.CreateWishes(tx, toUpdate, true)
		if err != nil {
			return fmt.Errorf("could not update wishes: %s", err)
		}
	}

	if args.RemoveMissing {
		var err error
		reply.Removed, err = db.GetDeletedWishIDs(tx, args.WishList, parsedRecipe.Wishes)
		if err != nil {
			return fmt.Errorf("could not get deleted wishes: %s", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit transaction: %s", err)
	}

	for _, wish := range reply.Created {
		// Trigger an update
		trigger, err := daemon.TriggerUpdateWish(nil, wish, false, "Recipe", "New wish from recipe")
		if err != nil {
			return fmt.Errorf("could not trigger update: %s", err)
		}
		reply.Triggers = append(reply.Triggers, trigger)
	}

	for _, wish := range reply.Updated {
		// Trigger an update
		trigger, err := daemon.TriggerUpdateWish(nil, wish, true, "Recipe", "Updated wish from recipe")
		if err != nil {
			return fmt.Errorf("could not trigger update: %s", err)
		}
		reply.Triggers = append(reply.Triggers, trigger)
	}

	if args.RemoveMissing {
		// Delete wishes that are not in the recipe
		// This is done by scheduling a delete operation
		for _, wish := range reply.Removed {
			trigger, err := daemon.TriggerDeleteWish(nil, wish.ID, "Recipe", "Wish not present in recipe")
			if err != nil {
				return fmt.Errorf("could not trigger delete: %s", err)
			}
			reply.Triggers = append(reply.Triggers, trigger)
		}
	}
	return nil
}
