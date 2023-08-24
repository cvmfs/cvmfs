package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/db"
)

const RpcAddress string = "/tmp/cvmfs/ducc/rpc.sock"

type CommandService struct{}

type AddWishArgs struct {
	ImageIdentifier string
	CvmfsRepository string

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
	wishInput, err := db.ParseImageURL(args.ImageIdentifier)
	if err != nil {
		return fmt.Errorf("invalid wish identifier: %s", err)
	}

	wishIdentifier := db.WishIdentifier{
		Source:                "cli",
		CvmfsRepository:       args.CvmfsRepository,
		InputTag:              wishInput.Tag,
		InputTagWildcard:      wishInput.TagWildcard,
		InputDigest:           wishInput.Digest,
		InputRepository:       wishInput.Repository,
		InputRegistryScheme:   wishInput.Scheme,
		InputRegistryHostname: wishInput.Registry,
	}

	wishes := []db.Wish{{Identifier: wishIdentifier, OutputOptions: args.OutputOptions, ScheduleOptions: args.ScheduleOptions}}
	wishes, err = db.CreateWishes(nil, wishes)
	if err != nil {
		return fmt.Errorf("could not create wish in DB: %s", err)
	}
	wish := wishes[0]

	// Trigger an update
	_, err = TriggerUpdateWish(nil, wish, false, "New Wish", "Triggered through CLI")
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

type UpdateWishArgs struct {
	ID                string
	ForceUpdateImages bool
}
type UpdateWishResponse struct {
	Success       bool
	MatchedWishes []db.Wish
	Trigger       db.Trigger
}

func (c *CommandService) UpdateWish(args UpdateWishArgs, reply *UpdateWishResponse) error {
	var err error
	reply.MatchedWishes, err = db.GetWishesByIDPrefix(nil, args.ID)
	if err != nil {
		return fmt.Errorf("could not get wishes from DB: %s", err)
	}
	if len(reply.MatchedWishes) == 0 {
		reply.Success = false
	}
	if len(reply.MatchedWishes) > 1 {
		reply.Success = false
	}
	trigger, err := TriggerUpdateWish(nil, reply.MatchedWishes[0], args.ForceUpdateImages, "cli", "Triggered through CLI")
	if err != nil {
		return err
	}
	reply.Success = true
	reply.Trigger = trigger
	return nil
}

func RunRpcServer(ctx context.Context, done chan<- any) {
	if err := os.MkdirAll(filepath.Dir(RpcAddress), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory for database: %s\n", err)
		os.Exit(1)
	}

	if err := os.Remove(RpcAddress); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "Error removing old socket: %s\n", err)
		os.Exit(1)
	}

	cmd := new(CommandService)
	rpc.Register(cmd)

	listener, err := net.Listen("unix", RpcAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating socket: %s\n", err)
		os.Exit(1)
	}
	defer os.Remove(RpcAddress)

	go func() {
		for {
			conn, err := listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				close(done)
				return
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "Error accepting connection: %s\n", err)
				continue
			}
			rpc.ServeConn(conn)
		}
	}()

	<-ctx.Done()
	listener.Close()
}
