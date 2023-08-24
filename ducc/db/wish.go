package db

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

// TODO: Add update intervals to wishes
const wishSqlFields string = "id, source, cvmfs_repository, input_tag, input_tag_wildcard, input_digest, input_repository, input_registry_scheme, input_registry_hostname, create_layers, create_flat, create_podman, create_thin, update_interval_sec, webhook_enabled"
const wishSqlFieldsPrefied string = "wishes.id, wishes.source, wishes.cvmfs_repository, wishes.input_tag, wishes.input_tag_wildcard, wishes.input_digest, wishes.input_repository, wishes.input_registry_scheme, wishes.input_registry_hostname, wishes.create_layers, wishes.create_flat, wishes.create_podman, wishes.create_thin, wishes.update_interval_sec, wishes.webhook_enabled"
const wishSqlFieldsQs string = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
const wishIdentifierSqlQueryTag string = "source=? AND cvmfs_repository=? AND input_tag=? AND input_tag_wildcard=? AND input_digest IS NULL AND input_repository=? AND input_registry_scheme=? AND input_registry_hostname=?"
const wishIdentifierSqlQueryDigest string = "source=? AND cvmfs_repository=? AND input_tag IS NULL AND input_tag_wildcard=? AND input_digest=? AND input_repository=? AND input_registry_scheme=? AND input_registry_hostname=?"
const wishIdentifierSqlFields string = "id, source, cvmfs_repository, input_tag, input_tag_wildcard, input_digest, input_repository, input_registry_scheme, input_registry_hostname"
const wishIdentifierSqlFieldsQs string = "?,?,?,?,?,?,?,?,?"

type WishID = uuid.UUID

type WishIdentifier struct {
	Source                string
	CvmfsRepository       string
	InputTag              string
	InputTagWildcard      bool
	InputDigest           digest.Digest
	InputRepository       string
	InputRegistryScheme   string
	InputRegistryHostname string
}

func (w WishIdentifier) InputString() string {
	if w.InputTag != "" {
		return fmt.Sprintf("%s://%s/%s:%s", w.InputRegistryScheme, w.InputRegistryHostname, w.InputRepository, w.InputTag)
	} else {
		return fmt.Sprintf("%s://%s/%s@%s", w.InputRegistryScheme, w.InputRegistryHostname, w.InputRepository, w.InputDigest)
	}
}

type WishOutputOptions struct {
	CreateLayers    ValueWithDefault[bool]
	CreateFlat      ValueWithDefault[bool]
	CreatePodman    ValueWithDefault[bool]
	CreateThinImage ValueWithDefault[bool]
}

func DefaultWishOutputOptions() WishOutputOptions {
	return WishOutputOptions{
		CreateLayers:    ValueWithDefault[bool]{Value: config.DEFAULT_CREATELAYERS, IsDefault: true},
		CreateFlat:      ValueWithDefault[bool]{Value: config.DEFAULT_CREATEFLAT, IsDefault: true},
		CreatePodman:    ValueWithDefault[bool]{Value: config.DEFAULT_CREATEPODMAN, IsDefault: true},
		CreateThinImage: ValueWithDefault[bool]{Value: config.DEFAULT_CREATETHINIMAGE, IsDefault: true},
	}
}

type WishScheduleOptions struct {
	WebhookEnabled ValueWithDefault[bool]
	UpdateInterval ValueWithDefault[time.Duration]
}

func DefaultWishScheduleOptions() WishScheduleOptions {
	return WishScheduleOptions{
		WebhookEnabled: ValueWithDefault[bool]{Value: config.DEFAULT_WEBHOOKENABLED, IsDefault: true},
		UpdateInterval: ValueWithDefault[time.Duration]{Value: config.DEFAULT_UPDATEINTERVAL, IsDefault: true},
	}
}

type Wish struct {
	ID              WishID
	Identifier      WishIdentifier
	OutputOptions   WishOutputOptions
	ScheduleOptions WishScheduleOptions
}

// CreateWishByIdentifier creates a wish in the database from a wish identifier.
// All options will be set to NULL, which means that the default values will be used.
// NB! This does not check if the wish already exists in the database.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateWishByIdentifier(tx *sql.Tx, identifier WishIdentifier) (Wish, error) {
	ws, err := CreateWishesByIdentifiers(tx, []WishIdentifier{identifier})
	if err != nil {
		return Wish{}, err
	}
	return ws[0], nil
}

// CreateWishesByIdentifiers creates wishes in the database from a slice of wish identifiers.
// All options will be sett to NULL, which means that the default values will be used.
// NB! This does not check if the wishes already exist in the database.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateWishesByIdentifiers(tx *sql.Tx, identifiers []WishIdentifier) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "INSERT INTO wishes (" + wishIdentifierSqlFields + ") VALUES (" + wishIdentifierSqlFieldsQs + ") RETURNING " + wishSqlFields

	// Prepare the statement
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	createdWishes := make([]Wish, 0, len(identifiers))
	for _, identifier := range identifiers {
		// Generate a new ID
		var wishId WishID
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err

		}
		wishId = WishID(id)
		// Handle nullable values
		input_tag := sql.NullString{String: identifier.InputTag, Valid: identifier.InputTag != ""}
		input_digest := sql.NullString{String: identifier.InputDigest.String(), Valid: identifier.InputDigest != digest.Digest("")}

		row := prepStmnt.QueryRow(wishId, identifier.Source, identifier.CvmfsRepository, input_tag, identifier.InputTagWildcard, input_digest, identifier.InputRepository, identifier.InputRegistryScheme, identifier.InputRegistryHostname)
		w, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		createdWishes = append(createdWishes, w)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return createdWishes, nil
}

func CreateWishes(tx *sql.Tx, wishes []Wish) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "INSERT INTO wishes (" + wishSqlFields + ") VALUES (" + wishSqlFieldsQs + ") RETURNING " + wishSqlFields
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	createdWishes := make([]Wish, 0, len(wishes))
	for _, wish := range wishes {

		// If an ID is not set, generate a new one
		var wishID = wish.ID
		if wishID == WishID(uuid.Nil) {
			id, err := uuid.NewRandom()
			if err != nil {
				return nil, err
			}
			wishID = WishID(id)
		}

		// Handle nullable values
		inputTag := sql.NullString{String: wish.Identifier.InputTag, Valid: wish.Identifier.InputTag != ""}
		inputDigest := sql.NullString{String: wish.Identifier.InputDigest.String(), Valid: wish.Identifier.InputDigest != digest.Digest("")}
		createLayers := sql.NullBool{Bool: wish.OutputOptions.CreateLayers.Value, Valid: !wish.OutputOptions.CreateLayers.IsDefault}
		createFlat := sql.NullBool{Bool: wish.OutputOptions.CreateFlat.Value, Valid: !wish.OutputOptions.CreateFlat.IsDefault}
		createPodman := sql.NullBool{Bool: wish.OutputOptions.CreatePodman.Value, Valid: !wish.OutputOptions.CreatePodman.IsDefault}
		createThin := sql.NullBool{Bool: wish.OutputOptions.CreateThinImage.Value, Valid: !wish.OutputOptions.CreateThinImage.IsDefault}
		updateInterval := sql.NullInt64{Int64: int64(wish.ScheduleOptions.UpdateInterval.Value.Seconds()), Valid: !wish.ScheduleOptions.UpdateInterval.IsDefault}
		webhookEnabled := sql.NullBool{Bool: wish.ScheduleOptions.WebhookEnabled.Value, Valid: !wish.ScheduleOptions.WebhookEnabled.IsDefault}

		row := prepStmnt.QueryRow(wishID, wish.Identifier.Source, wish.Identifier.CvmfsRepository, inputTag, wish.Identifier.InputTagWildcard, inputDigest, wish.Identifier.InputRepository, wish.Identifier.InputRegistryScheme, wish.Identifier.InputRegistryHostname, createLayers, createFlat, createPodman, createThin, updateInterval, webhookEnabled)
		w, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		createdWishes = append(createdWishes, w)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return createdWishes, nil
}

func GetAllWishes(tx *sql.Tx) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmnt string = "SELECT " + wishSqlFields + " from wishes ORDER BY id ASC"
	rows, err := tx.Query(stmnt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	wishes, err := parseWishesFromRows(rows)
	if err != nil {
		return nil, err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return wishes, nil
}

// GetWishByID returns a wish by its ID
// If the wish is not found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishByID(tx *sql.Tx, id WishID) (Wish, error) {
	w, err := GetWishesByIDs(tx, []WishID{id})
	if err != nil {
		return Wish{}, err
	}
	return w[0], nil
}

// GetWishesByIDs takes in a slice of wish IDs and returns a slice of wishes from the database.
// Unless all wishes are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesByIDs(tx *sql.Tx, ids []WishID) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	wishes := make([]Wish, 0, len(ids))
	stmnt := "SELECT " + wishSqlFields + " from wishes WHERE id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	for _, id := range ids {
		row := prepStmnt.QueryRow(id)
		w, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		wishes = append(wishes, w)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return wishes, nil
}

// GetWishByValue takes in a wish identifier and returns a wish from the database.
// If the wish is not found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishByValue(tx *sql.Tx, identifier WishIdentifier) (Wish, error) {
	wishes, err := GetWishesByValue(tx, []WishIdentifier{identifier})
	if err != nil {
		return Wish{}, err
	}
	if len(wishes) != 1 {
		return Wish{}, err
	}
	if wishes[0] == nil {
		return Wish{}, sql.ErrNoRows
	}
	return *wishes[0], nil
}

// GetWishesByValue takes in a slice of wish identifiers and returns a slice of wish pointers from the database.
// If a wish is not found, the corresponding pointer will be nil.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesByValue(tx *sql.Tx, identifiers []WishIdentifier) ([]*Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmntTag := "SELECT " + wishSqlFields + " from wishes WHERE " + wishIdentifierSqlQueryTag
	prepStmntTag, err := tx.Prepare(stmntTag)
	if err != nil {
		return nil, err
	}
	defer prepStmntTag.Close()
	stmntDigest := "SELECT " + wishSqlFields + " from wishes WHERE " + wishIdentifierSqlQueryDigest
	prepStmntDigest, err := tx.Prepare(stmntDigest)
	if err != nil {
		return nil, err
	}
	defer prepStmntDigest.Close()

	wishes := make([]*Wish, 0, len(identifiers))
	for _, identifier := range identifiers {

		var row *sql.Row
		if identifier.InputDigest != "" {
			row = prepStmntDigest.QueryRow(identifier.Source, identifier.CvmfsRepository, identifier.InputTagWildcard, identifier.InputDigest.String(), identifier.InputRepository, identifier.InputRegistryScheme, identifier.InputRegistryHostname)
		} else {
			row = prepStmntTag.QueryRow(identifier.Source, identifier.CvmfsRepository, identifier.InputTag, identifier.InputTagWildcard, identifier.InputRepository, identifier.InputRegistryScheme, identifier.InputRegistryHostname)
		}

		wish, err := parseWishFromRow(row)
		if errors.Is(err, sql.ErrNoRows) {
			wishes = append(wishes, nil)
		} else if err != nil {
			return nil, err
		} else {
			wishes = append(wishes, &wish)
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return wishes, nil
}

// GetWishesBySource takes in a source and returns a slice of all wishes from the database with that source.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesBySource(tx *sql.Tx, source string) ([]Wish, error) {
	stmnt := "SELECT " + wishSqlFields + " from wishes WHERE source = $1"

	var res *sql.Rows
	var err error
	if tx == nil {
		res, err = g_db.Query(stmnt, source)
	} else {
		res, err = tx.Query(stmnt, source)
	}

	if err != nil {
		return nil, err
	}
	defer res.Close()

	wishes, err := parseWishesFromRows(res)
	if err != nil {
		return nil, err
	}
	return wishes, nil
}

// DeleteWishesBySource deletes all wishes from the database with the given source.
// Unless all wishes are deleted, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func DeleteWishesByIDs(tx *sql.Tx, ids []WishID) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stmnt := "DELETE FROM wishes WHERE id = ?"

	// Prepare the statement
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, id := range ids {
		res, err := prepStmnt.Exec(id)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return sql.ErrNoRows
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func GetWishesByIDPrefix(tx *sql.Tx, idPrefix string) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmnt string = "SELECT " + wishSqlFields + " from wishes WHERE id LIKE ? ORDER BY id ASC"
	rows, err := tx.Query(stmnt, idPrefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out, err := parseWishesFromRows(rows)
	if err != nil {
		return nil, err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// parseWishesFromRows takes in a sql.Rows and Scans it into a wish.
// If any options contain NULL, the default values will be used.
// The row must contain the exact fields in the wishSqlFieldsOrdered constant, in the same order.
func parseWishFromRow(row scannableRow) (Wish, error) {
	w := Wish{}
	var input_tag, input_digest sql.NullString
	var create_layers, create_flat, create_podman, create_thin, webhook_enabled sql.NullBool
	var update_interval_sec sql.NullInt64
	err := row.Scan(&w.ID, &w.Identifier.Source, &w.Identifier.CvmfsRepository, &input_tag, &w.Identifier.InputTagWildcard, &input_digest, &w.Identifier.InputRepository, &w.Identifier.InputRegistryScheme, &w.Identifier.InputRegistryHostname, &create_layers, &create_flat, &create_podman, &create_thin, &update_interval_sec, &webhook_enabled)
	if err != nil {
		return Wish{}, err
	}

	// Handle the null values
	if input_tag.Valid {
		w.Identifier.InputTag = input_tag.String
	}
	if input_digest.Valid {
		w.Identifier.InputDigest, _ = digest.Parse(input_digest.String)
		// TODO: Handle invalid database state
	}
	if update_interval_sec.Valid {
		w.ScheduleOptions.UpdateInterval = ValueWithDefault[time.Duration]{Value: time.Duration(update_interval_sec.Int64) * time.Second, IsDefault: false}
	} else {
		w.ScheduleOptions.UpdateInterval = ValueWithDefault[time.Duration]{Value: config.DEFAULT_UPDATEINTERVAL, IsDefault: true}
	}
	w.OutputOptions.CreateLayers = nullBoolToValueWithDefault(create_layers, config.DEFAULT_CREATELAYERS)
	w.OutputOptions.CreateFlat = nullBoolToValueWithDefault(create_flat, config.DEFAULT_CREATEFLAT)
	w.OutputOptions.CreatePodman = nullBoolToValueWithDefault(create_podman, config.DEFAULT_CREATEPODMAN)
	w.OutputOptions.CreateThinImage = nullBoolToValueWithDefault(create_thin, config.DEFAULT_CREATETHINIMAGE)

	w.ScheduleOptions.WebhookEnabled = nullBoolToValueWithDefault(webhook_enabled, config.DEFAULT_WEBHOOKENABLED)

	return w, nil
}

// parseWishesFromRows takes in a sql.Rows and Scans it into a slice of wishes.
// If any options contain NULL, the default values will be used.
// The rows must contain the exact fields in the wishSqlFieldsOrdered constant, in the same order.
// Unless all rows are successfully parsed, an error is returned.
func parseWishesFromRows(rows *sql.Rows) ([]Wish, error) {
	wishes := make([]Wish, 0)
	for rows.Next() {
		w, err := parseWishFromRow(rows)
		if err != nil {
			return nil, err
		}
		wishes = append(wishes, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return wishes, nil
}

// sortWishesByID sorts a slice of wishes by their ID.
// NB! This modifies the slice in place.
func sortWishesByID(wishes []Wish) {
	sort.Slice(wishes, func(i, j int) bool {
		return wishes[i].ID.String() < wishes[j].ID.String()
	})
}
