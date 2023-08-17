package daemon

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/products"
	"github.com/cvmfs/ducc/registry"
	"github.com/google/uuid"
)

var entityBusyCv = sync.Cond{L: &sync.Mutex{}}
var entityBusy = make(map[uuid.UUID]any)

var runningChecksMutex = sync.Cond{L: &sync.Mutex{}}
var runningChecksByEntity = make(map[uuid.UUID]*db.Check)

func TriggerCheck(entityID uuid.UUID, checkType db.OperationType, triggerType db.TriggerType, details string) error {
	lockEntity(entityID)
	defer unlockEntity(entityID)

	trigger := db.CheckTrigger{
		CheckType: checkType,
		EntityID:  entityID,

		Type:      triggerType,
		Timestamp: time.Now(),
		Details:   details,
	}

	var err error
	trigger, err = db.CreateCheckTrigger(nil, trigger)
	if err != nil {
		return err
	}

	if err := processTriggersForEntity(entityID); err != nil {
		return err
	}

	return nil
}

func FinishCheck(check db.Check, status db.CheckStatus) error {
	lockEntity(check.EntityID)
	defer unlockEntity(check.EntityID)

	// Remove the check from the running checks map, so we can start a new one for this entity
	runningChecksMutex.L.Lock()
	delete(runningChecksByEntity, check.EntityID)
	runningChecksMutex.L.Unlock()

	// Mark check as done in DB
	check, err := db.UpdateStatusForCheckById(nil, check.ID, status)
	if err != nil {
		return err
	}
	// The check just finished, so now we can reschedule a new one for this entity and check type.
	if err := rescheduleCheck(check.EntityID, check.Type); err != nil {
		return err
	}

	// Since we just finished a check for this entity, it is possible that we can now start a new one.
	if err := processTriggersForEntity(check.EntityID); err != nil {
		return err
	}

	return nil
}

func restartCheck(tx *sql.Tx, check db.Check) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	switch check.Type {
	case db.OP_TYPE_EXPAND_WILDCARDS:
		// Get the associated wish
		wish, err := db.GetWishByID(tx, check.EntityID)
		if err != nil {
			return err
		}
		// Start the check

		go performWildCardCheckInternal(wish, &check)
	case db.OP_TYPE_UPDATE_IMAGE:
		// Get the associated image
		image, err := db.GetImageByID(tx, check.EntityID)
		if err != nil {
			return err
		}
		go performImageManifestCheckInternal(image, &check)
	default:
		return fmt.Errorf("unknown check type %s", check.Type)
	}

	// Reschedule this check. We want to run it again after a certain amount of time.
	rescheduleCheck(check.EntityID, check.Type)

	lockEntity(check.EntityID)
	defer unlockEntity(check.EntityID)

	// Check if there is already a check running for this entity. This should never happen.
	runningChecksMutex.L.Lock()
	runningCheck, exists := runningChecksByEntity[check.EntityID]
	if exists {
		runningChecksMutex.L.Unlock()
		return fmt.Errorf("check %s is already running for entity %s", runningCheck.ID.String(), check.EntityID.String())
	}
	runningChecksMutex.L.Unlock()

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// NB! Requires that the caller holds the entity lock
func startCheck(entityID uuid.UUID, checkType db.OperationType) error {
	check := db.Check{
		EntityID: entityID,
		Type:     checkType,
		Status:   db.OP_STATUS_RUNNING,
	}

	// Reschedule this check. We want to run it again after a certain amount of time.
	rescheduleCheck(entityID, checkType)

	// Check if there is already a check running for this entity. This should never happen.
	runningChecksMutex.L.Lock()
	runningCheck, exists := runningChecksByEntity[entityID]
	if exists {
		runningChecksMutex.L.Unlock()
		return fmt.Errorf("check %s is already running for entity %s", runningCheck.ID.String(), entityID.String())
	}
	runningChecksMutex.L.Unlock()

	// Perform DB operations in a transaction
	tx, err := db.GetTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Get all triggers for this entity and type
	triggers, err := db.GetPendingCheckTriggersForEntityIDFilterByType(tx, entityID, []db.OperationType{checkType})
	if err != nil {
		return err
	}
	if check, err = db.CreateCheck(tx, check); err != nil {
		return err
	}
	// Update the triggers to point to the check
	triggerIDs := make([]uuid.UUID, len(triggers))
	for i, trigger := range triggers {
		triggerIDs[i] = trigger.ID
	}
	if err := db.LinkTriggersToCheck(tx, check.ID, triggerIDs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	runningChecksMutex.L.Lock()
	// Add the check to the running checks map
	runningChecksByEntity[entityID] = &check
	runningChecksMutex.L.Unlock()

	return nil
}

// NB! Requires that the caller holds the entity lock
func processTriggersForEntity(entityID uuid.UUID) error {
	pendingTriggerTypes, err := db.GetPendingCheckTriggerTypesForEntityByID(nil, entityID)
	if err != nil {
		return err
	}
	if len(pendingTriggerTypes) == 0 {
		return nil
	}

	if _, exists := runningChecksByEntity[entityID]; exists {
		// There is already a check running for this entity, so we can't start a new one
		return nil
	}

	// Select a random trigger type, in case we have more than one type pending
	idx := rand.Intn(len(pendingTriggerTypes))
	triggerType := pendingTriggerTypes[idx]

	if err := startCheck(entityID, triggerType); err != nil {
		return err
	}

	return nil
}

func lockEntity(entityID uuid.UUID) {
	entityBusyCv.L.Lock()
	for _, busy := entityBusy[entityID]; busy; {
		entityBusyCv.Wait()
	}
	entityBusy[entityID] = true
	entityBusyCv.L.Unlock()
}

func unlockEntity(entityID uuid.UUID) {
	entityBusyCv.L.Lock()
	delete(entityBusy, entityID)
	entityBusyCv.Broadcast()
	entityBusyCv.L.Unlock()
}

func performImageManifestCheckInternal(image db.Image, check *db.Check) error {
	// Find what wishes reference this image
	wishes, err := db.GetWishesByImageID(nil, image.ID)
	if err != nil {
		FinishCheck(*check, db.CHECK_STATUS_FAILED)
		return err
	}
	if len(wishes) == 0 {
		// No wishes reference this image, so we don't want to check it
		FinishCheck(*check, db.OP_STATUS_SUCCESS)
		return nil
	}

	// Fetch and parse the manifest
	manifest, _, _, err := registry.FetchAndParseManifestAndList(image)
	if err != nil {
		FinishCheck(*check, db.CHECK_STATUS_FAILED)
	}

	// For each wish, check what products need to be created
	missingProducts := make([]db.WishOutputOptions, len(wishes))

	// TODO: Lock CVMFS for reading while we do this
	for i, wish := range wishes {
		if wish.OutputOptions.CreateLayers.Value {
			// Check that all layers are ok
			if ok, err := products.CheckLayers(wish.Identifier.CvmfsRepository, manifest); err != nil || !ok {
				missingProducts[i].CreateLayers.Value = true
			}
		}

		if wish.OutputOptions.CreateFlat.Value {
			// Check that the flat image is ok
			if ok, err := products.CheckFlatImage(wish.Identifier.CvmfsRepository, image, manifest); err != nil || !ok {
				missingProducts[i].CreateFlat.Value = true
			}
		}

	}

	return errors.New("not implemented")
}

func performWildCardCheckInternal(wish db.Wish, check *db.Check) error {
	created, updated, deleted, err := registry.ExpandWildcardAndStoreImages(wish)
	if err != nil {
		check.Status = db.CHECK_STATUS_FAILED
	}

	// New images should be immediately checked
	for _, image := range created {
		if err := TriggerCheck(image.ID, db.OP_TYPE_UPDATE_IMAGE, db.TRIGGER_TYPE_NEW_IMAGE, fmt.Sprintf("Image referenced by wish %s", wish.ID)); err != nil {
			if err := FinishCheck(*check, db.CHECK_STATUS_FAILED); err != nil {
				return err
			}
		}
	}

	// Updated images already have a check scheduled, but we reschedule it
	// in case this wish has a lower update interval than the previous one.
	for _, image := range updated {
		if err := scheduleManifestCheck(nil, image); err != nil {
			if err := FinishCheck(*check, db.CHECK_STATUS_FAILED); err != nil {
				return err
			}
		}
	}

	// Deleted images should be unscheduled
	for _, image := range deleted {
		scheduledUpdates.RemoveEntity(image.ID)
	}

	// Mark the check as done
	if err := FinishCheck(*check, db.OP_STATUS_SUCCESS); err != nil {
		return err
	}

	return nil
}
