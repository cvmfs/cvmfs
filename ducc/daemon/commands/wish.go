package commands

import (
	"database/sql"
	"fmt"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
)

type AddWishArgs struct {
	ImageIdentifier string
	CvmfsRepository string
	Wishlist        string

	OutputOptions   db.WishOutputOptions
	ScheduleOptions db.WishScheduleOptions
}
type AddWishResponse = db.Wish

func (c *CommandService) AddWish(args AddWishArgs, response *AddWishResponse) error {

	// Sanitize input
	if !cvmfs.RepositoryExists(args.CvmfsRepository) {
		return fmt.Errorf("repository %s does not exist", args.CvmfsRepository)
	}
	if !args.ScheduleOptions.UpdateInterval.IsDefault && args.ScheduleOptions.UpdateInterval.Value < config.MIN_UPDATEINTERVAL {
		return fmt.Errorf("update interval cannot be shorter than %s", config.MIN_UPDATEINTERVAL.String())
	}
	wishInput, err := daemon.ParseImageURL(args.ImageIdentifier)
	if err != nil {
		return fmt.Errorf("invalid wish identifier: %s", err)
	}

	wishIdentifier := db.WishIdentifier{
		Wishlist:              args.Wishlist,
		CvmfsRepository:       args.CvmfsRepository,
		InputTag:              wishInput.Tag,
		InputTagWildcard:      wishInput.TagWildcard,
		InputDigest:           wishInput.Digest,
		InputRepository:       wishInput.Repository,
		InputRegistryScheme:   wishInput.Scheme,
		InputRegistryHostname: wishInput.Registry,
	}

	wishes := []db.Wish{{Identifier: wishIdentifier, OutputOptions: args.OutputOptions, ScheduleOptions: args.ScheduleOptions}}
	wishes, err = db.CreateWishes(nil, wishes, false)
	if err != nil {
		return fmt.Errorf("could not create wish in DB: %s", err)
	}
	wish := wishes[0]

	// Trigger an update
	_, err = daemon.TriggerUpdateWish(nil, wish, false, "New Wish", "Triggered through CLI")
	if err != nil {
		return fmt.Errorf("could not trigger update: %s", err)
	}

	*response = wish
	return nil
}

type ListWishesArgs struct {
}
type ListWishesResponse struct {
	Wishes []db.Wish
}

func (c *CommandService) ListWishes(args ListWishesArgs, reply *ListWishesResponse) error {
	wishes, err := db.GetAllWishes(nil)
	if err != nil {
		return fmt.Errorf("could not get wishes from DB: %s", err)
	}
	reply.Wishes = wishes
	return nil
}

type SyncWishesArgs struct {
	IDs               []string
	ForceUpdateImages bool
}
type SyncWishesResponse struct {
	Synced    []db.Wish
	NotFound  []string
	Ambiguous []AmbiguousWish
	Triggers  []db.Trigger
}

type AmbiguousWish struct {
	Input         string
	MatchedWishes []db.Wish
}

func (c *CommandService) UpdateWish(args SyncWishesArgs, reply *SyncWishesResponse) error {
	var err error
	reply.Synced, reply.NotFound, reply.Ambiguous, err = findWishesByIDPrefix(nil, args.IDs)
	if err != nil {
		return fmt.Errorf("could not get wishes from DB: %s", err)
	}
	reply.Triggers = make([]db.Trigger, 0)
	for _, wish := range reply.Synced {
		trigger, err := daemon.TriggerUpdateWish(nil, wish, args.ForceUpdateImages, "cli", "Triggered through CLI")
		if err != nil {
			return err
		}
		reply.Triggers = append(reply.Triggers, trigger)
	}
	return nil
}

type DeleteWishesArgs struct {
	IDs []string
}

type DeleteWishesResponse struct {
	Deleted   []db.Wish
	NotFound  []string
	Ambiguous []AmbiguousWish
	Error     []DeleteWishError
	Triggers  []db.Trigger
}

type DeleteWishError struct {
	Wish db.Wish
	Err  error
}

func (c *CommandService) DeleteWishes(args DeleteWishesArgs, reply *DeleteWishesResponse) error {
	var err error
	reply.Triggers = make([]db.Trigger, 0)
	reply.Deleted, reply.NotFound, reply.Ambiguous, err = findWishesByIDPrefix(nil, args.IDs)
	if err != nil {
		return fmt.Errorf("could not get wishes from DB: %s", err)
	}

	for _, wish := range reply.Deleted {
		trigger, err := daemon.TriggerDeleteWish(nil, wish.ID, "Recipe", "Wish not present in recipe")
		if err != nil {
			reply.Error = append(reply.Error, DeleteWishError{Wish: wish, Err: err})
		}
		reply.Triggers = append(reply.Triggers, trigger)
	}

	return nil
}

func findWishesByIDPrefix(tx *sql.Tx, idPrefixes []string) (found []db.Wish, notFound []string, ambiguous []AmbiguousWish, err error) {
	wishesByPrefix, err := db.GetWishesByIDPrefixes(tx, idPrefixes)
	if err != nil {
		return nil, nil, nil, err
	}

	// Since multiple prefixes can match the same wish, we need to deduplicate
	foundMap := make(map[db.WishID]db.Wish)

	notFound = make([]string, 0)
	ambiguous = make([]AmbiguousWish, 0)
	found = make([]db.Wish, 0)

	for i, wishes := range wishesByPrefix {
		if len(wishes) == 0 {
			notFound = append(notFound, idPrefixes[i])
		}
		if len(wishes) > 1 {
			ambiguous = append(ambiguous, AmbiguousWish{Input: idPrefixes[i], MatchedWishes: wishes})
		}
		foundMap[wishes[0].ID] = wishes[0]
	}

	for _, wish := range foundMap {
		found = append(found, wish)
	}

	return found, notFound, ambiguous, nil
}
